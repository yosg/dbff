#! /usr/bin/python
#-*- encoding=utf8 -*-
import MySQLdb
from MySQLdb.cursors import DictCursor, SSCursor
import Queue
import atexit
import cStringIO
import copy
import logging
import re
import shlex
import shutil
import subprocess
import os
import tempfile
import threading
import sys
import time

__version__ = "1.4.4"

class Comparer():
    VERSION = __version__

    def __init__(self, source_host, source_port, source_schema,
                 source_username, source_password,
                 target_host, target_port, target_schema,
                 target_username, target_password,
                 no_data, concurrency=4, whitelist=None, blacklist=None,
                 output_document=None, verbose=False):
        self.source_host = source_host
        self.source_port = int(source_port)
        self.source_schema = source_schema
        self.source_username = source_username
        self.source_password = source_password
        self.target_host = target_host
        self.target_port = int(target_port)
        self.target_schema = target_schema
        self.target_username = target_username
        self.target_password = target_password
        self.no_data = no_data
        self.concurrency = int(concurrency)
        if whitelist is not None and whitelist != "":
            self.whitelist = re.split('\s*,\s*', whitelist)
        else:
            self.whitelist = None
        if blacklist is not None and blacklist != "":
            self.blacklist = re.split('\s*,\s*', blacklist)
        else:
            self.blacklist = None
        if output_document and output_document != "-":
            self.output_document = open(output_document, "w")
        else:
            self.output_document = sys.stdout
        self.verbose = verbose
        self.logger = logging.getLogger("comparer")
        logging.basicConfig(level=logging.ERROR, stream=sys.stdout, format='[%(asctime)s] [%(levelname)s] %(message)s')
        if self.verbose:
            self.logger.level = logging.DEBUG
        self.manifest = []

    def start(self):
        start_time = time.time()

        if not self.source_schema or not self.target_schema:
            self.logger.warn('Source or target not specified, is this a mistake?')
            exit(128)

        result = self.build()

        if self.output_document is not None:
            self.output_document.write(result)
        self.logger.info('')
        self.logger.info('All complete in %0.4f seconds.', time.time() - start_time)

    def build(self):
        source = Database(
            (
                self.source_host,
                self.source_username,
                self.source_password,
                self.source_schema,
                self.source_port,
            ), self.logger
        )
        target = Database(
            (
                self.target_host,
                self.target_username,
                self.target_password,
                self.target_schema,
                self.target_port,
            ), self.logger
        )
        queue = Queue.Queue()
        self.logger.info('Building dbffer queue...')
        self.logger.info("Compare between MySQL server %s with %s", source.version, target.version)

        blacklist = self.blacklist
        for name, table in source.tables.items():
            if blacklist and re.match(blacklist, name):
                continue
            if name in target.tables:
                queue.put((table, target.tables[name]))
            else:
                queue.put((table, None))
        for name, table in target.tables.items():
            if blacklist and re.match(blacklist, name):
                continue
            if name not in source.tables:
                queue.put((None, table))
        self.logger.info('Starting dbffer...')

        workers = []
        buf = cStringIO.StringIO()
        l = threading.Lock()
        for i in xrange(self.concurrency):
            dbffer = Dbffer(source.clone(), target.clone(),
                            queue, buf, l,
                            no_data=self.no_data,
                            blacklist=blacklist,
                            whitelist=self.whitelist)
            dbffer.start()
            workers.append(dbffer)
        self.logger.info('Waiting for dbffer complete...')
        for w in workers:
            w.join()
        self.logger.info("Done!")

        if buf.getvalue() != '':
            self.logger.info('Dumping compare result...')
            return "\n".join([
                "/*!40101 SET NAMES utf8 */;",
                "",
                "/*!40101 SET SQL_MODE=''*/;",
                "",
                "/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;",
                "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;",
                "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;",
                "/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;",
                "",
                buf.getvalue(),
                "/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;",
                "/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;",
                "/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;",
                "/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;", "\n"
            ])
        else:
            self.logger.info('No difference between source and target.')
            return ""


class Dbffer(threading.Thread):
    def __init__(self, source, target, queue, buf, lock, default_character_set='utf8', blacklist=None, whitelist=None,
                 no_data=False, log_error=None):
        self.source = source
        self.target = target
        self.queue = queue
        self.default_character_set = default_character_set
        self.whitelist = whitelist
        self.blacklist = blacklist
        self.no_data = no_data
        self.log_error = log_error
        self.update = []
        self.delete = []
        self.insert = []
        self.truncate = []
        self.drop = []
        self.create = []
        self.alter = []
        self.buf = buf
        self.lock = lock
        threading.Thread.__init__(self)

    def dump(self, table, cursor=None):
        if table.rows > 0:
            if cursor is None:
                cursor = table.db.connection.cursor(SSCursor)
            fields = [column.field for column in table.columns]
            cursor.execute('SELECT * FROM `%s`' % table.name)
            rows = ','.join([str(Row(row, table, fields)) for row in cursor.fetchall()])
            self.insert.append('INSERT INTO `%s` VALUES %s' % (table.name, rows))
        return table.rows

    def compare(self, source, target):
        (
            self.update, self.delete, self.insert,
            self.truncate, self.drop, self.create, self.alter
        ) = [], [], [], [], [], [], []
        no_data = self.no_data
        if self.whitelist is not None:
            if source and source.name not in self.whitelist or target and target.name not in self.whitelist:
                return
        if self.blacklist and not no_data:
            if source and source.name in self.blacklist or target and target.name in self.blacklist:
                no_data = True
        if source is None:
            self.drop.append('DROP TABLE `%s`' % target.name)
        elif target is None:
            cursor = self.source.connection.cursor(SSCursor)
            cursor.execute('SHOW CREATE TABLE `%s`;' % source.name)
            self.create.append('%s' % str(cursor.fetchall()[0][1]).replace('\n', ''))
            if not no_data:
                self.dump(source, cursor)
        else:
            do_not_compare = False
            columns_in_target = target.columns[:]
            dropped_columns = []
            for column in columns_in_target:
                if column not in source:
                    self.alter.append('DROP COLUMN `%s`' % column.field)
                    dropped_columns.append(column)
            for column in dropped_columns:
                columns_in_target.remove(column)

            columns_in_source = source.columns[:]
            for i in range(len(columns_in_source)):
                column = columns_in_source[i]
                if column not in target:
                    pos = 'AFTER `%s`' % columns_in_source[i - 1].field if i > 0 else 'FIRST'
                    self.alter.append('ADD COLUMN `%s` %s %s' % (column.field, column, pos))
                    columns_in_target.insert(i, column)
                elif len(columns_in_target) > i and column.field == columns_in_target[i].field:
                    if column.type == columns_in_target[i].type and \
                            column.null == columns_in_target[i].null and \
                            column.default == columns_in_target[i].default and \
                            column.extra == columns_in_target[i].extra and \
                            column.comment == columns_in_target[i].comment:
                        continue
                    pos = 'AFTER `%s`' % columns_in_source[i - 1].field if i > 0 else 'FIRST'
                    self.alter.append('MODIFY COLUMN `%s` %s %s' % (column.field, column, pos))
                else:
                    j = columns_in_target.index([item for item in columns_in_target if item.field == column.field][0])
                    columns_in_target[i], columns_in_target[j] = columns_in_target[j], columns_in_target[i]
                    pos = 'AFTER `%s`' % columns_in_source[i - 1].field if i > 0 else 'FIRST'
                    self.alter.append('MODIFY COLUMN `%s` %s %s' % (column.field, column, pos))

            for name in [name for name in target.indexes if name not in source.indexes]:
                if name == 'PRIMARY':
                    self.alter.append('DROP PRIMARY KEY')
                    do_not_compare = True
                else:
                    self.alter.append('DROP INDEX `%s`' % name)
                del target.indexes[name]
            for name in source.indexes:
                if name in target.indexes and source.indexes[name] != target.indexes[name]:
                    if name == 'PRIMARY':
                        do_not_compare = True
                        self.alter.append('DROP PRIMARY KEY')
                    else:
                        self.alter.append('DROP INDEX `%s`' % name)
                    del target.indexes[name]
                if name not in target.indexes:
                    definition = ['ADD']
                    if source.indexes[name][0].key_name == 'PRIMARY':
                        definition.append('PRIMARY KEY (`%s`)' %
                                          '`, `'.join([index.column_name for index in source.indexes[name]]))
                        do_not_compare = True
                    elif not source.indexes[name][0].non_unique:
                        definition.append('UNIQUE KEY `%s` USING %s' % (name, source.indexes[name][0].index_type))
                        definition.append('(`%s`)' % '`, `'.join([index.column_name for index in source.indexes[name]]))
                    else:
                        definition.append('INDEX `%s` USING %s' % (name, source.indexes[name][0].index_type))
                        definition.append('(`%s`)' % '`, `'.join([index.column_name for index in source.indexes[name]]))
                    if source.indexes[name][0].index_comment != '':
                        definition.append("COMMENT '%s'" % source.indexes[name][0].index_comment)
                    self.alter.append(' '.join(definition))

            if source.engine != target.engine:
                self.alter.append('ENGINE=%s' % source.engine)
            if source.comment != target.comment:
                self.alter.append("COMMENT='%s'" % source.comment)

            if not no_data:
                if do_not_compare:
                    self.truncate.append('TRUNCATE TABLE `%s`' % source.name)
                    self.dump(source)
                else:
                    fields_in_source = [column.field for column in source.columns]
                    fields_in_target = [column.field for column in target.columns]
                    if fields_in_target != fields_in_source:
                        fields_in_target = sorted(
                            set([column.field for column in target.columns]) & set(fields_in_source))
                        fields_in_source = sorted(fields_in_source)
                    rows_in_source = dict()

                    cursor = self.source.connection.cursor(SSCursor)
                    cursor.execute('SELECT `%s` FROM `%s`' % ('`,`'.join(fields_in_source), source.name))
                    for row in cursor.fetchall():
                        row = Row(row, source, fields_in_source)
                        rows_in_source[row.key] = row

                    cursor = self.target.connection.cursor(SSCursor)
                    cursor.execute('SELECT `%s` FROM `%s`' % ('`,`'.join(fields_in_target), target.name))

                    for row in cursor.fetchall():
                        row = Row(row, target, fields_in_target)
                        if row.key in rows_in_source:
                            if row != rows_in_source[row.key]:
                                query = ['UPDATE `%s`' % target.name, 'SET']

                                (buf, condition) = [], []
                                for k, v in enumerate(rows_in_source[row.key].seq):
                                    field = fields_in_source[k]
                                    if str(v) != str(row[field]):
                                        buf.append("`%s`=%s" % (field, Row.escape(v)))
                                query.append(', '.join(buf))

                                query.append('WHERE')
                                for field in source.pk_fields:
                                    condition.append("`%s`=%s" % (field, Row.escape(row[field])))
                                query.append(' AND '.join(condition))
                                self.update.append(' '.join(query))
                            del rows_in_source[row.key]
                        else:
                            query = ['DELETE FROM `%s`' % target.name, 'WHERE']
                            condition = []
                            for field in source.pk_fields:
                                condition.append("`%s`=%s" % (field, Row.escape(row[field])))
                            query.append(' AND '.join(condition))
                            self.delete.append(' '.join(query))
                    for key, row in rows_in_source.items():
                        self.insert.append(
                            'INSERT INTO `%s` (`%s`) VALUES %s' % (target.name, '`,`'.join(fields_in_source), str(row)))
        if self.drop or self.create or self.alter or self.truncate or self.update or self.delete or self.insert:
            self.lock.acquire()
            self.buf.write('/* SYNC TABLE : `%s` */\n' % (source.name if source is not None else target.name))
            if self.create:
                self.buf.write('%s;\n' % ';\n'.join(self.create))
            if self.drop:
                self.buf.write('%s;\n' % ';\n'.join(self.drop))
            if self.truncate:
                self.buf.write('%s;\n' % ';\n'.join(self.truncate))
            if self.alter:
                self.buf.write('ALTER TABLE `%s` %s;\n' % (source.name, ', '.join(self.alter)))
            if self.delete:
                self.buf.write('%s;\n' % ';\n'.join(self.delete))
            if self.update:
                self.buf.write('%s;\n' % ';\n'.join(self.update))
            if self.insert:
                self.buf.write('%s;\n' % ';\n'.join(self.insert))
            self.buf.write('\n')
            self.lock.release()
        return

    def run(self):
        while not self.queue.empty():
            try:
                (source, target) = self.queue.get(False)
                self.compare(source, target)
            except Queue.Empty:
                break
        return None


class Row(object):
    def __init__(self, seq, table, fields):
        self.fields = fields
        self.seq = seq
        self.key = tuple([self[field] for field in table.pk_fields])

    @staticmethod
    def escape(o):
        if o is None:
            return 'NULL'
        return ("'%s'" % MySQLdb.escape_string(str(o))).replace("\n", "\\n")

    def __str__(self):
        return '(%s)' % ','.join(map(Row.escape, self.seq))

    def __eq__(self, other):
        if type(other) != Row or self.key != other.key:
            return False
        if len(self.seq) != len(other.seq):
            return False
        for pair in zip(self.seq, other.seq):
            if str(pair[0]) != str(pair[1]):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def __getitem__(self, item):
        return self.seq[self.fields.index(item)] if item in self.fields else None


class Column(object):
    def __init__(self, definition):
        """
        @param self
        @param definition
        """
        (
            self.field,
            self.type,
            self.collation,
            self.null,
            self.key,
            self.default,
            self.extra,
            self.privileges,
            self.comment
        ) = definition
        if '\r' in self.comment:
            self.comment = self.comment.replace('\r', '')

    def __eq__(self, other):
        if self.field != other.field or self.type != other.type or self.null != other.null or \
                self.key != other.key or self.default != other.default or self.extra != other.extra or \
                self.comment != other.comment:
            return False
        return True

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        definition = [self.type]
        if self.null == 'NO':
            definition.append('NOT NULL')
        if self.default is not None:
            default = str(self.default)
            if self.default not in ['CURRENT_TIMESTAMP']:
                default = "'%s'" % default
            definition.append("DEFAULT %s" % default)
        if self.extra != '':
            definition.append(self.extra)
        if self.comment != '':
            definition.append("COMMENT '%s'" % MySQLdb.escape_string(self.comment).replace('\n', '\\n'))
        return ' '.join(definition)


class Index(object):
    def __init__(self, definition):
        """
        @param self
        @param definition
        """
        (
            self.table,
            self.non_unique,
            self.key_name,
            self.seq_in_index,
            self.column_name,
            self.collation,
            self.cardinality,
            self.sub_part,
            self.packed,
            self.null,
            self.index_type,
            self.comment,
            self.index_comment
        ) = definition

    def __eq__(self, other):
        if self.table != other.table or self.non_unique != other.non_unique or self.key_name != other.key_name or \
                self.seq_in_index != other.seq_in_index or self.column_name != other.column_name or \
                self.index_type != other.index_type or self.index_comment != other.index_comment:
            return False
        return True

    def __ne__(self, other):
        return not self == other

    def __str__(self):
        return '\n'.join([
            'table: %s' % self.table,
            'non_unique: %s' % self.non_unique,
            'key_name: %s' % self.key_name,
            'seq_in_index: %s' % self.seq_in_index,
            'column_name: %s' % self.column_name,
            'index_type: %s' % self.index_type,
            'index_comment: %s' % self.index_comment,
        ])


class Table(object):
    def __init__(self, name, db):
        """
        @param name str
        @param db   Database
        """
        self.db = db
        self.pk_fields = ()
        self.columns = []
        self.indexes = {}

        cursor = db.connection.cursor(SSCursor)
        cursor.execute("show table status like '%s'" % name)
        (
            self.name,
            self.engine,
            self.version,
            self.row_format,
            self.rows,
            self.avg_row_length,
            self.data_length,
            self.max_data_length,
            self.index_length,
            self.data_free,
            self.auto_increment,
            self.create_time,
            self.update_time,
            self.check_time,
            self.collation,
            self.checksum,
            self.create_options,
            self.comment
        ) = cursor.fetchall()[0]

        cursor.execute('show full columns from `%s`' % self.name)
        for row in cursor.fetchall():
            column = Column(row)
            self.columns.append(column)
        cursor.execute('show index from `%s`' % self.name)
        for index in [Index(row) for row in cursor.fetchall()]:
            if index.key_name not in self.indexes:
                self.indexes[index.key_name] = []
            self.indexes[index.key_name].append(index)
        if 'PRIMARY' in self.indexes:
            self.pk_fields = tuple([index.column_name for index in self.indexes['PRIMARY']])
        cursor.close()

    def __eq__(self, other):
        if self.name != other.name or self.engine != other.engine or self.comment != other.comment or \
                len(self.columns) != len(other.columns) or len(self.indexes) != len(other.indexes):
            return False
        for i, column in enumerate(self.columns):
            if not column.__eq__(other.columns[i]):
                return False
        for i, index in enumerate(self.indexes):
            if not index.__eq__(other.indexes[i]):
                return False
        return True

    def __ne__(self, other):
        return not self == other

    def __contains__(self, item):
        if type(item) == Column:
            for column in self.columns:
                if column.field == item.field:
                    return True
        return False


class Database(object):
    def __init__(self, server, logger, tables=None):
        """
        @param server
        """
        (
            self.host,
            self.username,
            self.password,
            self.name,
            self.port
        ) = server
        self.logger = logger
        self.logger.debug('Connecting to server %s...', self.host)
        self.connection = MySQLdb.connect(*server)
        self.tables = {}
        self.server_version = None

        self.logger.debug('Set connection charset utf8...')
        cursor = self.connection.cursor(DictCursor)
        cursor.execute('SET NAMES utf8')

        if tables is None:
            self.logger.debug('List tables from %s...', self.name)
            cursor.execute('SHOW TABLE STATUS FROM `%s` WHERE ENGINE IS NOT NULL' % self.name)
            for row in cursor.fetchall():
                table = Table(row['Name'], self)
                self.logger.debug('Found table %s', table.name)
                self.tables[table.name] = table
        else:
            self.tables = tables
        cursor.close()

    def __contains__(self, item):
        for table in self.tables.values():
            if table == item:
                return True
        return False

    @property
    def version(self):
        if self.server_version is None:
            cursor = self.connection.cursor(DictCursor)
            cursor.execute('SELECT VERSION() `version`')
            self.server_version = cursor.fetchone()['version']
        return self.server_version

    def clone(self):
        return Database((self.host, self.username, self.password, self.name, self.port), self.logger, copy.copy(self.tables))

    def close(self):
        self.connection.close()
        self.tables.clear()

