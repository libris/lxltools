# lddb
LIBRISXL Linked Data Database

## Setting up a postgresql database

1. Install postgresql.

On Mac, use homebrew to install. Make sure you have at least version 9.4.

    $ brew install postgresql

2. Create a user (optional)

    $ createuser -d -P <username> 

3. Create a database

    $ createdb -E UTF8  -O <user from step 2 (if used)> <databasename>

4. Create tables

    $ psql -U <username> <databasename> < config/lddb.sql

If you didn't create a user in step 2, you can ignore the "-U <username> part".


