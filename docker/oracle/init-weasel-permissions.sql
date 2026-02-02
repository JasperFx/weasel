-- Grant the weasel user permissions to create and drop schemas (users in Oracle)
-- This script runs after the APP_USER is created by the container

ALTER SESSION SET CONTAINER = FREEPDB1;

-- Grant privileges to create and drop users (schemas)
GRANT CREATE USER TO weasel;
GRANT DROP USER TO weasel;
GRANT ALTER USER TO weasel;

-- Grant ability to create sessions and manage objects in other schemas
GRANT CREATE SESSION TO weasel WITH ADMIN OPTION;
GRANT CREATE TABLE TO weasel WITH ADMIN OPTION;
GRANT CREATE SEQUENCE TO weasel WITH ADMIN OPTION;
GRANT CREATE PROCEDURE TO weasel WITH ADMIN OPTION;
GRANT CREATE VIEW TO weasel WITH ADMIN OPTION;

-- Grant unlimited tablespace so weasel can allocate space to schemas it creates
GRANT UNLIMITED TABLESPACE TO weasel WITH ADMIN OPTION;

-- Grant ability to select from system views for schema introspection
GRANT SELECT ON sys.all_tables TO weasel;
GRANT SELECT ON sys.all_tab_columns TO weasel;
GRANT SELECT ON sys.all_constraints TO weasel;
GRANT SELECT ON sys.all_cons_columns TO weasel;
GRANT SELECT ON sys.all_indexes TO weasel;
GRANT SELECT ON sys.all_ind_columns TO weasel;
GRANT SELECT ON sys.all_sequences TO weasel;
GRANT SELECT ON sys.all_users TO weasel;
GRANT SELECT ON sys.all_objects TO weasel;
