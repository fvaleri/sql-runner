-- Copyright 2018 Federico Valeri.
-- Licensed under the Apache License 2.0 (see LICENSE file).

drop table if exists users;
create table users (
    usr_userid varchar primary key,
    usr_password varchar,
    usr_email varchar
);

drop table if exists payments;
create table payments (
    pmt_code bigint primary key,
    pmt_int_code bigint,
    pmt_amount numeric(20, 2),
    pmt_date date,
    pmt_ch_code bigint,
    pmt_state bigint,
    pmt_account varchar,
    pmt_type varchar
);
