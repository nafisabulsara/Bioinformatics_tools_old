import psycopg2
from sshtunnel import SSHTunnelForwarder


class DBConnect(object):
    """Control connections to databases"""
    _instance = None
    keymds = "/Users/nbulsara/.ssh/id_mds"
    port = 5432
    localport = 5432
    servers = None
    tunnels = None

    def __new__(cls, srv):
        if cls._instance is None:
            cls._instance = object.__new__(cls)
            print("Have srv {}".format(srv))
            # Can add use of a config file elsewhere
        return cls._instance

    def __init__(self, srv):
        self.servers = {
            'mds': ['mds-production-db.cdk1nb86kzjj.us-east-1.rds.amazonaws.com',
                    'mds', 'read_only_user', '5gpedpU7UufvzRr2sz3m4x'],
            'mds-read-replica': ['mds-production-db-slave.cdk1nb86kzjj.us-east-1.rds.amazonaws.com',
                                 'mds', 'read_only_user', '5gpedpU7UufvzRr2sz3m4x'],
            'mds-validation': ['mds-validation.cdk1nb86kzjj.us-east-1.rds.amazonaws.com',
                               'mds', 'read_only_user', '5gpedpU7UufvzRr2sz3m4x']
        }
        # host: URL, user, [defined elsewhere]
        self.tunnels = {
            'mds': ['bastion.mds.genalyte.com',
                    'ubuntu', self.keymds, self.port, self.localport],
            'mds-read-replica': ['bastion.mds.genalyte.com',
                    'ubuntu', self.keymds, self.port, self.localport],
            'mds-validation': ['bastion.mds.genalyte.com',
                    'ubuntu', self.keymds, self.port, self.localport]
        }

        self.open_tunnel(srv)
        self.open_db(srv)
        self.tunnel = self._instance.tunnel
        self.connection = self._instance.connection
        self.cursor = self._instance.cursor

    def open_tunnel(self, srv):
        # Connecting with ssh tunnel
        SSH_HOST = self.tunnels[srv][0]
        SSH_USER = self.tunnels[srv][1]
        SSH_PORT = 22
        SSH_KEYFILE = self.tunnels[srv][2]
        SSH_FOREIGN_PORT = self.tunnels[srv][3]  # Port that postgres is running on the foreign server
        SSH_INTERNAL_PORT = self.tunnels[srv][4]  # Port we open locally that is forwarded to

        # Postgres Config

        DB_HOST = self.servers[srv][0]
        DB_PORT = self.tunnels[srv][3]
        DB_LOCAL = self.tunnels[srv][4]
        # print("Tunnel will be to host {}".format(DB_HOST))
        print("SSH tunnel local port {}, DB_HOST {}, DB_PORT {}".format(DB_LOCAL, DB_HOST, DB_PORT))

        try:
            DBConnect._instance.tunnel = SSHTunnelForwarder(
                (SSH_HOST, 22),
                ssh_pkey=SSH_KEYFILE,
                ssh_username=SSH_USER,
                remote_bind_address=(DB_HOST, DB_PORT),
                local_bind_address=('localhost', DB_LOCAL)
            )
            self.tunnel.start()
            print("SSH tunnel local port {}, DB_HOST {}, DB_PORT {}".format(DB_LOCAL, DB_HOST, DB_PORT))

        except Exception as e:
            print("SSH connection failed. Error: {}".format(e))

    def open_db(self, srv):
        DB_PORT = self.tunnels[srv][4]
        DB_PWRD = self.servers[srv][3]
        DB_NAME = self.servers[srv][1]
        DB_USER = self.servers[srv][2]
        print("--DB connect using--\nhost {}\nport {}\nDB name {}\nDB user {}"\
              .format(self.tunnel.local_bind_host,
                      self.tunnel.local_bind_port,
                      DB_NAME,DB_USER))

        try:
            connection = DBConnect._instance.connection = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                host=self.tunnel.local_bind_host,
                port=self.tunnel.local_bind_port,
                password=DB_PWRD
            )
            print("--Verify host--\n{}\nport {}".format(self.tunnel.local_bind_host, self.tunnel.local_bind_port))
            cursor = DBConnect._instance.cursor = connection.cursor()
            cursor.execute('SELECT VERSION()')
            db_version = cursor.fetchone()
            print("DB version {}".format(db_version))
        except Exception as error:
            print('Error: DB connection not established.\n{}'.format(error))
            DBConnect._instance = None
            self.tunnel.close()
            exit(-1)
        else:
            pass
        print(" established. \n-v {}".format(db_version[0]))
        return DBConnect._instance

    def get_cursor(self):
        return self.connection.cursor()

    def query(self, query):
        try:
            result = self.cursor.execute(query)
        except Exception as error:
            print('Error: failed query "{}" [{}]'.format(query, error))
            return None
        else:
            return result

    # def __del__(self):
    #     self.connection.close()
    #     self.tunnel.close()

