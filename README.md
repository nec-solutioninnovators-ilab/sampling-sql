# Sampling-SQL

## Description

Sampling-SQL adds a sample aggregation function to DBMSs with large data. This software is provided in the format of a JDBC driver and operates as a proxy software that exists between the user and the DBMS.

See [User's Guide](./doc/UsersGuide.md) for more information.

## Building from source

Clone from git.

    git clone https://github.com/nec-solutioninnovators-ilab/sampling-sql.git

Build with Maven.

    cd sampling-sql
    mvn clean
    mvn package

Sampling-SQL jar file will be generated in the path below:

    sampling-sql/target/sampling-sql-0.0.1-SNAPSHOT-jar-with-dependencies.jar

## License

This software is released under the MIT License, see `LICENSE`.

