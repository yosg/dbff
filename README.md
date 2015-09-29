# dbff
Compare MySQL tables and rows between databases.

基于MySQL的数据库差异比对工具，支持表结构、数据行的差异比较，导出标准的SQL文件。

```
usage: dbff [-h] [--source-host HOST] [--source-port PORT]
            [--source-username USERNAME] [--source-password PASSWORD]
            [--source-schema SCHEMA] [--target-host HOST] [--target-port PORT]
            [--target-username USERNAME] [--target-password PASSWORD]
            [--target-schema SCHEMA] [-d] [--whitelist TABLES]
            [--blacklist TABLES] [--default-character-set CHARSET_NAME]
            [--log-error FILE] [-O FILE] [-v] [--version]

loris database comparer v1.4.0

optional arguments:
  -h, --help            show this help message and exit
  --source-host HOST
  --source-port PORT
  --source-username USERNAME
  --source-password PASSWORD
  --source-schema SCHEMA
  --target-host HOST
  --target-port PORT
  --target-username USERNAME
  --target-password PASSWORD
  --target-schema SCHEMA
  -d, --no-data         do not write any table row information.
  --whitelist TABLES    specify tables that will be included
  --blacklist TABLES    specify tables that will be excluded
  --default-character-set CHARSET_NAME
                        set the default character set
  --log-error FILE      append warnings and errors to given file
  -O FILE, --output-document FILE
                        output file name
  -v, --verbose         print extra information
  --version             output version information and exit

Report bugs to: hi@xiayi.li

e.g.
./dbff --source-host=192.168.1.6 --source-port=3306 --source-username=root --source-password=123456 --source-schema=mysql \
       --target-host=192.168.1.8 --target-port=3306 --target-username=root --target-password=123456 --target-schema=mysql \
       -O/tmp/mysql_6-8.sql -t16

```
