# data-platform-infrastructure

This repository contains all relevant files and information about the storage infrastructure of the 3PhaseInsight Data Platform.
We are using [sqitch](https://sqitch.org/) to manage Database Schema Migrations.

## Sqitch (on MacOS)

Run
```
brew tap sqitchers/sqitch
brew install sqitch --with-postgres-support
```

Make sure the user `threephi_db_user` is set up before running the database migrations.
User setup is not part of the migrations since it would require having a password being picked up somewhere.
This would impose the use of specific infrastructure to deal with secret-storage, which should be left up to the 
Platform Operator.

Here is a suggestion in case the user is created manually by a System Admin:
```
CREATE ROLE threephi_db_user WITH
  LOGIN
  NOSUPERUSER
  NOCREATEDB
  NOCREATEROLE
  INHERIT
  NOREPLICATION
  PASSWORD 'strong_password_here';
```

Once the user is set up, navigate to [sqitch](./sqitch) and run:
```
./sqitch-deploy.sh <db_hostname> <db_user> <db_password> <optional:db_name>
```

This will create all required tables on your Database.