#  SQL templates

sql_create_stage_customers = """
    drop table if exists stage_customers;
    create table stage_customers
            (
                id integer,
                name varchar(512),
                birth_date date not null,
                gender varchar(1),
                email varchar(128)
            );
    """

sql_create_stage_goods = """
    drop table if exists stage_goods;
    create table stage_goods
        (
            id integer,
            name text,
            price numeric
        );
    """

sql_create_stage_orders = """
    drop table if exists stage_orders;
    create table stage_orders
    (
        id integer,
        uuid uuid,
        good_name varchar(512),
        order_date timestamp,
        amount integer,
        customer_name varchar(512),
        customer_email varchar(128)
    );
    """

sql_create_stage_payment_status = """
    drop table if exists stage_payment_status;
    create table stage_payment_status
    (
        order_uuid uuid,
        status boolean
    );
"""

# pg to pg customers
sql_get_src_customers = "select id, trim(name), birth_date, gender, trim(email) from customers"
sql_insert_target_customers = "insert into stage_customers values %s"

# pg to pg goods
sql_get_src_goods = "select id, trim(name), price from goods"
sql_insert_target_goods = "insert into stage_goods values %s"

# Final dataset
sql_load_final_dataset = """
    create table if not exists final_dataset
    (
        name varchar(512),
        age double precision,
        good_title varchar(512),
        order_date timestamp,
        payment_status boolean,
        total_price numeric,
        amount integer,
        last_modified_at timestamp with time zone
    );

    truncate table final_dataset;

    insert into final_dataset(name, age, good_title, order_date, payment_status, total_price, amount, last_modified_at) 
    with _orders as (
        select   uuid
                ,good_name
                ,order_date
                ,amount
                ,customer_name
                ,trim(upper(customer_email)) as customer_email
                ,row_number() over (partition by uuid order by order_date desc) as rn
          from stage_orders
    )
    ,_customers as (
        select   c.email
                ,date_part('year',age(c.birth_date)) as age
                ,c.name
                ,row_number() OVER (partition by c.email order by c.name) as rn
          from stage_customers c
    )
    ,_goods as (
        select   name
                ,price
                ,row_number() over (partition by name order by name) as rn
          from stage_goods
    )
    ,_payment_status as (
        select   order_uuid
                ,status
                ,row_number() over (partition by order_uuid order by case status
                                                                        when TRUE
                                                                            then 1
                                                                        else 99 end) as rn
          from stage_payment_status
    )

    select
             coalesce(c.name, o.customer_name) as name
            ,coalesce(c.age, -1) as age
            ,o.good_name as good_title
            ,o.order_date
            ,p.status as payment_status
            ,o.amount * g.price as total_price
            ,o.amount
            ,now() as last_modified_at
      from _orders o
      left join _customers c
             on trim(upper(c.email)) = trim(upper(o.customer_email))
            and c.rn = 1
      left join _goods g
             on g.name = o.good_name
            and g.rn = 1
      left join _payment_status p
             on p.order_uuid = o.uuid
            and p.rn = 1
     where o.rn = 1;
"""