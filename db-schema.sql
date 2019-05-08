-- The DB schema for the test:

create table test (
	id serial primary key,
	name varchar(100) not null,
	added timestamp not null
);

create index test_idx on test(name);

