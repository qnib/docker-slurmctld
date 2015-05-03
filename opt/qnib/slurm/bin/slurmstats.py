#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""

Usage:
    slurmstats.py [options]

Options:
    --delay <int>           Seconds delay inbetween loop runs [default: 10]
    --loop                  Loop the execution infinitely
    --neo4j-host <str>      Neo4j host [default: neo4j.service.consul]

Generic Options:
    --loglevel, -L=<str>    Loglevel [default: INFO]
                            (ERROR, CRITICAL, WARN, INFO, DEBUG)
    --log2stdout, -l        Log to stdout, otherwise to logfile. [default: False]
    --logfile, -f=<path>    Logfile to log to (default: <scriptname>.log)
    --cfg, -c=<path>        Configuration file.
    -h --help               Show this screen.
    --version               Show version.

"""

# load librarys
import traceback
import logging
import os
import re
import grp
import time
import codecs
import ast
import sys
import consul
import graphitesend
from ClusterShell.NodeSet import NodeSet
import envoy
from pprint import pprint
from ConfigParser import RawConfigParser, NoOptionError
from requests.exceptions import ConnectionError
from neo4jrestclient.client import GraphDatabase, Node
from neo4jrestclient.query import QuerySequence
import networkx as nx
import matplotlib.pyplot as plt



try:
    from docopt import docopt
except ImportError:
    HAVE_DOCOPT = False
else:
    HAVE_DOCOPT = True

__author__ = 'Christian Kniep <christian()qnib.org>'
__copyright__ = 'Copyright 2015 QNIB Solutions'
__license__ = """GPL v2 License (http://www.gnu.org/licenses/old-licenses/gpl-2.0.en.html)"""


class QnibConfig(RawConfigParser):
    """ Class to abstract config and options
    """
    specials = {
        'TRUE': True,
        'FALSE': False,
        'NONE': None,
    }

    def __init__(self, opt):
        """ init """
        RawConfigParser.__init__(self)
        if opt is None:
            self._opt = {
                "--log2stdout": False,
                "--logfile": None,
                "--loglevel": "ERROR",
            }
        else:
            self._opt = opt
            self.logformat = '%(asctime)-15s %(levelname)-5s [%(module)s] %(message)s'
            self.loglevel = opt['--loglevel']
            self.log2stdout = opt['--log2stdout']
            if self.loglevel is None and opt.get('--cfg') is None:
                print "please specify loglevel (-L)"
                sys.exit(0)
            self.eval_cfg()

        self.eval_opt()
        self.set_logging()
        logging.info("SetUp of QnibConfig is done...")


    def do_get(self, section, key, default=None):
        """ Also lent from: https://github.com/jpmens/mqttwarn
            """
        try:
            val = self.get(section, key)
            if val.upper() in self.specials:
                return self.specials[val.upper()]
            return ast.literal_eval(val)
        except NoOptionError:
            return default
        except ValueError:  # e.g. %(xxx)s in string
            return val
        except:
            raise
            return val


    def config(self, section):
        ''' Convert a whole section's options (except the options specified
                explicitly below) into a dict, turning

                    [config:mqtt]
                    host = 'localhost'
                    username = None
                    list = [1, 'aaa', 'bbb', 4]

                into

                    {u'username': None, u'host': 'localhost', u'list': [1, 'aaa', 'bbb', 4]}

                Cannot use config.items() because I want each value to be
                retrieved with g() as above
            SOURCE: https://github.com/jpmens/mqttwarn
            '''

        d = None
        if self.has_section(section):
            d = dict((key, self.do_get(section, key))
                     for (key) in self.options(section) if key not in ['targets'])
        return d


    def eval_cfg(self):
        """ eval configuration which overrules the defaults
            """
        cfg_file = self._opt.get('--cfg')
        if cfg_file is not None:
            fd = codecs.open(cfg_file, 'r', encoding='utf-8')
            self.readfp(fd)
            fd.close()
            self.__dict__.update(self.config('defaults'))


    def eval_opt(self):
        """ Updates cfg according to options """

        def handle_logfile(val):
            """ transforms logfile argument
                """
            if val is None:
                logf = os.path.splitext(os.path.basename(__file__))[0]
                self.logfile = "%s.log" % logf.lower()
            else:
                self.logfile = val

        self._mapping = {
            '--logfile': lambda val: handle_logfile(val),
        }
        for key, val in self._opt.items():
            if key in self._mapping:
                if isinstance(self._mapping[key], str):
                    self.__dict__[self._mapping[key]] = val
                else:
                    self._mapping[key](val)
                break
            else:
                if val is None:
                    continue
                mat = re.match("\-\-(.*)", key)
                if mat:
                    self.__dict__[mat.group(1)] = val
                else:
                    logging.info("Could not find opt<>cfg mapping for '%s'" % key)


    def set_logging(self):
        """ sets the logging """
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.DEBUG)
        if self.log2stdout:
            hdl = logging.StreamHandler()
            hdl.setLevel(self.loglevel)
            formatter = logging.Formatter(self.logformat)
            hdl.setFormatter(formatter)
            self._logger.addHandler(hdl)
        else:
            hdl = logging.FileHandler(self.logfile)
            hdl.setLevel(self.loglevel)
            formatter = logging.Formatter(self.logformat)
            hdl.setFormatter(formatter)
            self._logger.addHandler(hdl)


    def __str__(self):
        """ print human readble """
        ret = []
        for key, val in self.__dict__.items():
            if not re.match("_.*", key):
                ret.append("%-15s: %s" % (key, val))
        return "\n".join(ret)

    def __getitem__(self, item):
        """ return item from opt or __dict__
        :param item: key to lookup
        :return: value of key
        """
        if item in self.__dict__.keys():
            return self.__dict__[item]
        else:
            return self._opt[item]

    def __setitem__(self, key, value):
        """ set item to
        """
        self.__dict__[key] = value


class Neo4j(object):
    """" Class to abstract interactions
    """
    def __init__(self, cfg):
        """ Init object
        """
        self._cfg = cfg
        self.con_gdb()
        self._labels = {}

    def con_gdb(self):
        """ connect to neo4j
        """
        url = "http://%(--neo4j-host)s:7474" % self._cfg
        try:
            self._gdb = GraphDatabase(url)
        except ConnectionError:
            time.sleep(3)
            self.con_gdb()

    def query(self, query):
        self._cfg._logger.debug(query)
        res = self._gdb.query(query, returns=Node)
        src_ret = self.unfold(res)
        return src_ret

    def unfold(self, res):
        if all([isinstance(item[0], Node) for item in res]):
            return [item[0] for item in res]
        elif isinstance(res, list):
            ret = res.pop()
            return self.unfold(ret)
        else:
            if isinstance(res, QuerySequence):
                return None
            return res

    def create_node(self, label, **kwargs):
        """ create node
        """
        if label not in self._labels:
            self._labels[label] =  self._gdb.labels.create(label)
        node = self._gdb.nodes.create(**kwargs)
        self._labels[label].add(node)
        return node

    @staticmethod
    def have_relationship(label, dst, src):
        """ Checks if node is member of partition
        :param gnode: Neo4j object of Node
        :param gpart: Neo4j object of Partition
        :return: True if member, else False
        """
        for rel in dst.relationships:
            if rel.type == label and rel.start.properties['name'] == src.properties['name']:
                return True
        return False


class NetX(object):
    """ Holds networkx graph
    """
    def __init__(self, cfg):
        self._cfg = cfg
        self._sw_map = {
            "odd": "sw1",
            "even": "sw0",
            }

    def __str__(self):
        """ Prints edges and nodes
        :return: string
        """
        txt = []
        for edge in self._graph.edges():
            txt.append("%s -> %s" % edge)
        return "\n".join(txt)


    def bootstrap(self):
        """ initiaize CLOS2 graph
        """
        self._graph = nx.DiGraph()
        # Hard coded CLOS2 network with odd nodes on sw1, even nodes on sw0
        self._graph.add_node("root", color="blue", type="switch")
        for key, val in self._sw_map.items():
            self._graph.add_node(val, color="blue", partition=key, type="switch")
            self._graph.add_edge("root", val)
            self._cfg._logger.info("add root->%s" % val)

    def dump(self, path="/tmp/network.gpickle"):
        self._cfg._logger.debug("%-5s > Dump graph to '%s'" % (traceback.extract_stack(None, 2)[0][1], path))
        nx.write_gpickle(self._graph, path)

    def restore(self, path="/tmp/network.gpickle"):
        """ restore graph from file
        """
        self._cfg._logger.debug("%-5s > Restore graph from '%s'" % (traceback.extract_stack(None, 2)[0][1], path))
        self._graph = nx.read_gpickle(path)

    def change_color(self, node, color, jobid=""):
        """ change color of given node
        :param node: node name
        :param color: color
        """
        self._graph.node[node]["color"] = color
        self._graph.node[node]["jobid"] = jobid


    def draw(self, path="/var/www/slurm/", fname="jobs.png", plain=True, jobid="", replace=True):
        # positions for all nodes
        if not replace and os.path.exists(os.path.join(path, fname)):
            return
        pos = nx.graphviz_layout(self._graph, prog='twopi', args='')
        sw_names = [item[0] for item in self._graph.nodes(data=True) if item[1]['type'] == "switch"]
        sw_colors = [item[1]['color'] for item in self._graph.nodes(data=True) if item[1]['type'] == "switch"]
        if plain:
            labels = dict((item[0], item[0]) for item in self._graph.nodes(data=True))
        else:
            labels = dict((item[0], item[0]) for item in self._graph.nodes(data=True) if item[1]['type'] == "switch")
            labels.update(dict((item[0], item[0]) for item in self._graph.nodes(data=True) if item[1]['type'] == "compute" and item[1]['jobid'] == ""))
            if jobid == "":
                labels.update(dict((item[0], "%s\nJobID: %s" % (item[0], item[1]['jobid'])) for item in self._graph.nodes(data=True) if item[1]['type'] == "compute" and item[1]['jobid'] != ""))
            else:
                labels.update(dict((item[0], item[0]) for item in self._graph.nodes(data=True) if item[1]['type'] == "compute" and item[1]['jobid'] != jobid))
                labels.update(dict((item[0], "%s\nJobID: %s" % (item[0], item[1]['jobid'])) for item in self._graph.nodes(data=True) if item[1]['type'] == "compute" and item[1]['jobid'] == jobid))

        com_nodes = {
            "busy": {},
            "idle": {}
        }
        for item in self._graph.nodes(data=True):
            if item[1]['type'] != "compute":
                continue
            if item[1]['jobid'] != "":
                key = "busy"
            else:
                key = "idle"
            if jobid == "":
                com_nodes[key][item[0]] = item[1]["color"]
            elif item[1]["jobid"] == jobid:
                com_nodes[key][item[0]] = item[1]["color"]
            else:
                com_nodes[key][item[0]] = "grey"


        # nodes
        nx.draw_networkx_nodes(self._graph, pos, node_size=1200, nodelist=sw_names, node_color=sw_colors)
        nx.draw_networkx_nodes(self._graph, pos, node_size=4000, nodelist=com_nodes["busy"].keys(), node_color=com_nodes["busy"].values())
        nx.draw_networkx_nodes(self._graph, pos, node_size=3600, nodelist=com_nodes["idle"].keys(), node_color=com_nodes["idle"].values())

        # edges
        nx.draw_networkx_edges(self._graph, pos)

        # labels
        nx.draw_networkx_labels(self._graph, pos, labels=labels, font_size=12, font_family='sans-serif')
        # save as png
        self._cfg._logger.debug("savefig(%s)" % os.path.join(path, fname))
        plt.savefig(os.path.join(path, fname))
        plt.clf()

    def add_partition_edge(self, pname, node):
        """ Adds link between partition switch and node
        """
        if pname not in self._sw_map.keys():
            return
        sw_name = self._sw_map[pname]
        self._cfg._logger.info("add %s->%s" % (sw_name, node))
        self._graph.add_node(node, color="grey", type="compute", jobid="")
        self._graph.add_edge(sw_name, node)


class SlurmStats(object):
    """ Fetch SLURM statistics and push them to the metric system
    """

    def __init__(self, cfg):
        """ Init of instance
        """
        self._cfg = cfg
        self._consul = consul.Consul()
        self._jobs = Jobs(self._cfg)
        self._n4j = Neo4j(cfg)
        self._last_run = time.time()
        netx = NetX(cfg)
        netx.bootstrap()
        netx.dump()

    def loop(self):
        """  loop over run
        """
        while True:
            self.run()
            now = time.time()
            since = now - self._last_run
            delay = max(0, int(self._cfg['--delay']) - since)
            self._cfg._logger.info("Took: %ssec, sleep for %ssec" % (since, delay))
            self._last_run = now
            time.sleep(delay)

    def run(self):
        """ do the work
        """
        self.eval_partiton()
        self.eval_job()

    def eval_partiton(self):
        """ Evaluate partition
        """
        proc = envoy.run("scontrol show partitions")
        partitions = []
        partition = None
        for line in proc.std_out.split("\n"):
            if line.startswith("PartitionName"):
                if partition is not None:
                    partitions.append(partition)
                partition = SctlPartition(self._cfg, self._consul, self._n4j)
            partition.eval_line(line)
        if partition is None:
            return
        partitions.append(partition)
        for item in partitions:
            item.push()
        netx = NetX(self._cfg)
        netx.restore()
        netx.draw(fname="partition.png", plain=True)
        del netx

    def cache_users(self):
        """ build hashmap with users and node references, to help build up relationships

        passed around through cfg instance
        """
        nodes = self._n4j.query()


    def eval_job(self):
        """ Evaluate job output
        """
        proc = envoy.run("scontrol show jobs")
        self._jobs.wipe()
        job = None
        for line in proc.std_out.split("\n"):
            if line.startswith("JobId"):
                if job is not None:
                    self._jobs.append(job)
                job = SctlJob(self._cfg, self._consul, self._n4j)
            elif job is None:
                self._jobs.push()
                return
            job.eval_line(line)
        self._jobs.append(job)
        self._jobs.push()


class Jobs(object):
    """ Class that aggregates all Jobs
    """
    def __init__(self, cfg):
        """ initialise stats
        """
        self._cfg = cfg
        self.jobs = []
        self.con_gsend()
        self._graph_color = ["green", "purple", "orange", "red", "yellow", "lightblue"]

    def con_gsend(self):
        """ connect to graphite in a loop
        """
        try:
            self._gsend = graphitesend.init(graphite_server='carbon.service.consul',
                                        prefix='slurm',
                                        system_name='stats')
        except graphitesend.GraphiteSendException:
            time.sleep(5)
            self.con_gsend()

        self.wipe()

    def wipe(self):
        """ clean counters
        """
        # per user
        self._users = {}
        # per group
        self._groups = {}

    def append(self, job):
        """ sum up stats
        """
        self.jobs.append(job)
        if job._info['JobState'] not in ("RUNNING", "PENDING"):
            return
        # per user
        user = job._info['User']
        if user not in self._users:
            self._users[user] = {
                "RUNNING": {
                    "jobs": 0,
                    "nodes": 0,
                    "cpus": 0,
                    },
                "PENDING": {
                    "jobs": 0,
                    "nodes": 0,
                    "cpus": 0,
                    },
                }
        self._users[user][job._info['JobState']]["jobs"] += 1
        self._users[user][job._info['JobState']]["nodes"] += int(job._info['NumNodes'])
        self._users[user][job._info['JobState']]["cpus"] += int(job._info['NumCPUs'])
        # per groups
        for group in [g.gr_name for g in grp.getgrall() if user in g.gr_mem]:
            if group not in self._groups:
                self._groups[group] = {
                    "RUNNING": {
                        "jobs": 0,
                        "nodes": 0,
                        "cpus": 0,
                    },
                    "PENDING": {
                        "jobs": 0,
                        "nodes": 0,
                        "cpus": 0,
                    },
                }
            self._groups[group][job._info['JobState']]["jobs"] += 1
            self._groups[group][job._info['JobState']]["nodes"] += int(job._info['NumNodes'])
            self._groups[group][job._info['JobState']]["cpus"] += int(job._info['NumCPUs'])

    def push(self):
        """ push to backend
        """
        self.push_graphite()
        color_map = [item for item in self._graph_color]
        for job in self.jobs:
            job.push()
            if len(color_map) == 0:
                # TODO: Dirty hack to prevent slurmstats from crashing...
                color_map = [item for item in self._graph_color]
            color = color_map.pop()
            if not job.add_to_graph(color):
                # if False, job was not added (!= RUNNING)
                color_map.append(color)

        netx = NetX(self._cfg)
        netx.restore()
        for job in self.jobs:
            if job._info["JobState"] == "RUNNING":
                netx.draw(fname="job_%(JobId)s.png" % job._info, plain=False, jobid=job._info["JobId"], replace=False)

        netx.draw(fname="jobs.png", plain=False)

    def push_graphite(self):
        """ Push stuff to carbon
        """
        self._cfg._logger.debug("Push slurm stats per user")
        for user, states in self._users.items():
            for state, stats in states.items():
                for stat, val in stats.items():
                    key = "users.%s.%s.%s" % (user, state, stat)
                    self._cfg._logger.debug("> %s %s" % (key, val))
                    self._gsend.send(key, val)
        self._cfg._logger.debug("Push slurm stats per group")
        for group, states in self._groups.items():
            for state, stats in states.items():
                for stat, val in stats.items():
                    key = "groups.%s.%s.%s" % (group, state, stat)
                    self._cfg._logger.debug("> %s %s" % (key, val))
                    self._gsend.send(key, val)


class SctlJob(object):
    """ Class to build up information about a job provided by scontrol
    """
    def __init__(self, cfg, consul_cli, n4j):
        self._cfg = cfg
        self._info = {}
        self._consul = consul_cli
        self._n4j = n4j

    def eval_line(self, line):
        """ enriches object to evaluate scrontol output
        :param line: stdout of 'scontrol show jobs'
        """
        for item in line.split():
            try:
                key, val = item.split("=")
            except ValueError:
                continue
            if key == "NodeList" and val == "(null)":
                val = None
            if key == "NumNodes" and re.match("\d+\-\d+", val):
                val = val.split("-")[0]
            elif key in ("UserId", "GroupId"):
                mat = re.match("(\w+)\((\d+)\)", val)
                name, val = mat.groups()
                if key == "UserId":
                    self._info["User"] = name
                else:
                    self._info["Group"] = name
            self._info[key] = val

    def __str__(self):
        """ human readable outout
        :return: string describing job
        """
        txt = [
            "#### JobId:%(JobId)-3s User:%(UserId)s" % self._info,
            "Requeue:%(Requeue)1s BatchFlag:%(BatchFlag)1s CPUs:%(NumCPUs)-3s Nodes:%(NumNodes)-3s CPUs/Task:%(CPUs/Task)s" % self._info,
            "Partition:%(Partition)-10s NodeList:%(NodeList)s" % self._info,

            ]
        return "\n".join(txt)

    def push(self):
        """ push information to backend
        """
        #self.push_consul()
        self.push_neo4j()

    def add_to_graph(self, color):
        """ color nodes of RUNNING jobs
        """
        if self._info['JobState'] != "RUNNING":
            return False
        netx = NetX(self._cfg)
        netx.restore()
        for node in NodeSet(self._info['NodeList']):
            netx.change_color(node, color, self._info["JobId"])
        netx.dump()
        return True

    def push_consul(self):
        """ push information to consul backend
        """
        # get
        idx, items = self._consul.kv.get("slurm/partition/%(Partition)s", recurse=True)
        partitions = {}
        for item in items:
            mat = re.match(".*/(\w+)/nodes", item['Key'])
            if mat:
                 partitions[mat.group(1)] = item['Value'].split(",")
        # Push
        #self._consul.kv.put("slurm/job/%(JobId)s/nodes" % self._info, self._info['TotalNodes'])

    def push_neo4j(self):
        """ populate neo4j with job information

        (n:Node {name:<node_name>})-[:PART_OF]->(j:Job {jobid:<jobid>, name:<jobname>})
        """
        query = "MATCH (j:Job) WHERE j.jobid='%(JobId)s' RETURN j" % self._info
        gjob = self._n4j.query(query)
        if len(gjob) == 0:
            self._cfg._logger.info("No job found, create '%(JobId)s'" % self._info)
            gjob = self._n4j.create_node("Job", jobid=self._info['JobId'], name=self._info['JobName'], state=self._info['JobState'])
        else:
            # since we got a list back... it has to be an one-item list
            gjob = gjob.pop()
        if gjob.properties['state'] != self._info['JobState']:
            self._cfg._logger.warn("Jobstate '%s' has changed... %s -> %s" % (self._info['JobId'], gjob.properties['state'], self._info['JobState']))
            gjob['state'] = self._info['JobState']


        #if not self._n4j.have_relationship("PART_OF", gjob, gnode):
        #    gnode.relationships.create("PART_OF", gjob)

        for node in NodeSet(self._info['NodeList']):
            query = "MATCH (n:Node) WHERE n.name='%s' RETURN n" % node
            gnode = self._n4j.query(query)
            if len(gnode) == 0:
                self._cfg._logger.info("No node found, create '%s'" % node)
                gnode = self._n4j.create_node("Node", name=node)
                gnode.relationships.create("PART_OF", gjob)
            else:
                # since we got a list back... it has to be an one-item list
                gnode = gnode.pop()
            if not self._n4j.have_relationship("PART_OF", gjob, gnode):
                gnode.relationships.create("PART_OF", gjob)


class SctlPartition(object):
    """ Class to build up information about a partition provided by scontrol
    """
    def __init__(self, cfg, consul_cli, n4j):
        self._cfg = cfg
        self._info = {}
        self._consul = consul_cli
        self._n4j = n4j

    def eval_line(self, line):
        """ enriches object to evaluate scrontol output
        :param line: stdout of 'scontrol show partations'
        """
        for item in line.split():
            key, val = item.split("=")
            if val.startswith(","):
                val = val[1:]
            self._info[key] = val

    def __str__(self):
        """ human readable outout
        :return: string describing job
        """
        txt = [
            "#### Partition:%(PartitionName)-3s TotalCPUs:%(TotalCPUs)-3s TotalNodes:%(TotalNodes)-3s" % self._info,
            "Nodes:%(Nodes)s" % self._info,
        ]
        return "\n".join(txt)

    def push(self):
        """ push information to backend
        """
        self.push_consul()
        self.push_neo4j()

    def push_consul(self):
        """ push information to consul backend
        """
        try:
            self._consul.kv.put("slurm/partition/%(PartitionName)s/total_nodes" % self._info, self._info['TotalNodes'])
            self._consul.kv.put("slurm/partition/%(PartitionName)s/total_cpus" % self._info, self._info['TotalCPUs'])
            self._consul.kv.put("slurm/partition/%(PartitionName)s/state" % self._info, self._info['State'])
            self._consul.kv.put("slurm/partition/%(PartitionName)s/nodes" % self._info, self._info['Nodes'])
        except KeyError:
            pass

    def push_neo4j(self):
        """ populate neo4j with partition information

        (n:Node {name:<node_name>})-[:MEMBER]->(p:Partition {name:<partition_name>})
        """
        netx = NetX(self._cfg)
        netx.restore()
        query = "MATCH (p:Partition) WHERE p.name='%(PartitionName)s' RETURN p" % self._info
        gpart = self._n4j.query(query)
        if len(gpart) == 0:
            self._cfg._logger.info("No partition found, create '%s'" % gpart)
            gpart = self._n4j.create_node("Partition", name=self._info['PartitionName'])
        else:
            gpart = gpart.pop()
        if self._info['Nodes'] is None:
            return
        for node in NodeSet(self._info['Nodes']):
            query = "MATCH (n:Node) WHERE n.name='%s' RETURN n" % node
            gnode = self._n4j.query(query)
            if len(gnode) == 0:
                self._cfg._logger.info("No node found, create '%s'" % node)
                gnode = self._n4j.create_node("Node", name=node)
                gpart.relationships.create("MEMBER", gnode)
            else:
                gnode = gnode.pop()
            netx.add_partition_edge(self._info['PartitionName'], node)
            if not self.is_partition_member(gnode, gpart):
                gpart.relationships.create("MEMBER", gnode)
        netx.dump()


    @staticmethod
    def is_partition_member(gnode, gpart):
        """ Checks if node is member of partition
        :param gnode: Neo4j object of Node
        :param gpart: Neo4j object of Partition
        :return: True if member, else False
        """
        for rel in gnode.relationships:
            if rel.type == "MEMBER" and rel.start.properties['name'] == gpart.properties['name']:
                return True
        return False



def main():
    """ main function """
    options = None
    if HAVE_DOCOPT:
        options = docopt(__doc__, version='Test Script 0.1')
    qcfg = QnibConfig(options)
    slurmstat = SlurmStats(qcfg)

    if qcfg['--loop']:
        slurmstat.loop()
    else:
        slurmstat.run()


if __name__ == "__main__":
    main()
