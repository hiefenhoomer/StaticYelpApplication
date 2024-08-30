do $$ declare
    r RECORD;
begin
    for r in (select tablename from pg_tables where schemaname = 'public') loop
        execute 'drop table if exists ' || quote_ident(r.tablename) || ' cascade';
    end loop;
end $$;

create table "attributes" (
    "attribute" varchar(50) primary key,
    value varchar(20) not null
);

create table categories (
    category varchar(50) primary key
);

create table zipcode (
    zipcode varchar(5) primary key,
    mean_income int not null,
    median_income int not null,
    population int not null
);

create table business (
    business_id varchar(100) primary key,
    fk_zipcode char(5) not null,
    name varchar(100) not null,
    address varchar(200) not null,
    city varchar(100) not null,
    state varchar(100) not null,
    longitude decimal(11,8),
    latitude decimal(11,8),
    stars decimal(2,1),
    total_stars int,
    num_checkins int,
    review_count int,
    foreign key (fk_zipcode) references zipcode(zipcode)
);

create table business_hours (
    fk_business_id varchar(100) not null,
    day_of_week varchar(10) not null,
    hours varchar(20),
    primary key (fk_business_id, day_of_week),
    foreign key (fk_business_id) references business(business_id)
);

create table business_categories (
    fk_business_id varchar(100) not null,
    fk_category varchar(50) not null,
    primary key (fk_business_id, fk_category),
    foreign key (fk_business_id) references business(business_id),
    foreign key (fk_category) references categories(category)
);

create table business_attributes (
    fk_business_id varchar(100) not null,
    fk_attribute varchar(50) not null,
    primary key (fk_business_id, fk_attribute),
    foreign key (fk_business_id) references business(business_id),
    foreign key (fk_attribute) references Attributes(attribute)
);

create table checkin_day (
    day_id serial primary key,
    fk_business_id varchar(100) not null,
    day varchar(10),
    unique (fk_business_id, day),
    foreign key (fk_business_id) references business(business_id)
);

create table checkin_hour (
    time_id serial primary key,
    fk_day_id int not null,
    hour time not null,
    total_checkins int not null,
    foreign key (fk_day_id) references checkin_day(day_id)
);

create table yelp_user (
    user_id varchar(100) primary key,
    name varchar(100) not null,
    yelping_since date,
    review_count int,
    fans int,
    average_stars decimal(3,2),
    useful int,
    funny int,
    cool int
);

create table review (
    review_id varchar(100),
    fk_user_id varchar(100) not null,
    fk_business_id varchar(100) not null,
    date date,
    stars int,
    text text,
    useful int,
    funny int,
    cool int,
    primary key (review_id),
    foreign key (fk_business_id) references business(business_id),
    foreign key (fk_user_id) references yelp_user(user_id)
);

create table friend (
    fk_user_id varchar(100) not null,
    fk_friend_id varchar(100) not null,
    primary key (fk_friend_id, fk_user_id),
    foreign key (fk_user_id) references yelp_user(user_id),
    foreign key (fk_friend_id) references yelp_user(user_id)
);

/*create view avg_income_global as
-- Collapse all incomes in zipcode on avg_income.
select
    avg(avg_income) as avg_income_global
from
    zipcode;

create view global_income_business as
select
    b.business_id,
    b.name,
    b.zipcode,
    z.avg_income,
    g.avg_income_global,
    -- get the income difference.
    (z.avg_income - g.avg_income_global) as income_difference
from
    business b
join
    zipcode z
on
-- join on same zipcode.
    b.zipcode = z.zipcode
-- Have to cross join here so every column has the global average associated with it.
cross join
    avg_income_global g;


-- Overpriced is this:
-- $: - Not overpriced
-- $$ - Overpriced if zip average is < global average - 5000
-- $$$ - overpriced if zip avg < global avg + 5000
create view is_overpriced as
select
    gb.business_id, gb.name, gb.zipcode, gb.avg_income, gb.avg_income_global, gb.income_difference,
    a.value as price_rating,
    -- If else statements end as var means var = aggregate of conditionals
    case
        when a.value = '3' and gb.income_difference > 5000 then 'no'
        when a.value = '2' and gb.income_difference > -5000 then 'no'
        when a.value = '1' then 'no'
        else 'yes' end as overpriced
    from
        global_income_business gb
    join
    -- Get the many to many relationship.
        business_attributes ba on gb.business_id = ba.fk_business_id
    join
        "attributes" a on ba.fk_attribute = a."attribute"
    where
        a."attribute" = 'RestaurantsPriceRange2';
*/

-- Sort all businesses into popularity.
/*create view popular_businesses as
    select
        b.business_id, b.name, bc.total_checkins, b.zipcode,
        row_number() over (partition by b.zipcode order by bc.total_checkins desc) as rank
    from
        show_business_checkins bc
    join
        business b on b.business_id = bc.fk_business_id;

-- Get the get the top 10 popular businesses per zip for better representation.
create view top_10_popular_businesses_per_zip as
    select
        business_id,
        name,
        zipcode,
        total_checkins
    from
        popular_businesses
    where
        rank <= 10;

-- Helper view
create view business_total_checkins_days_count as
    select
        b.business_id, b.name, b.zipcode, sum(ch.count) as total_checkins, count(distinct ch.day) as total_days
    from
        checkin ch
    join
        business b on b.business_id = ch.fk_business_id
    group by
        b.business_id,
        b.name,
        b.zipcode;

-- Successful businesses are defined as businesses with the highest daily customers.
-- The top 10 are taken from every zip
create view top_10_successful_bussinesses_per_zip as
with business_daily_customers as (
    select
        business_id, avg_ch.name,
        avg_ch.zipcode,
        (avg_ch.total_checkins / avg_ch.total_days) as avg_daily_customers,
        row_number() over (partition by avg_ch.zipcode order by (avg_ch.total_checkins / avg_ch.total_days) desc) as rank
    from business_total_checkins_days_count avg_ch
)
select
    business_id,
    name,
    zipcode,
    avg_daily_customers
from
    business_daily_customers
where
    rank <= 10;
*/

create or replace function update_total_average_count_stars()
returns trigger as $update_business_stars$
    begin
        update business
        set total_stars = total_stars + new.stars,
            review_count = review_count + 1,
            stars = round(cast(total_stars + new.stars as numeric) / cast(business.review_count + 1 as numeric), 1)
        where business_id = new.fk_business_id;
        return new;
    end;
$update_business_stars$ language plpgsql;

drop trigger if exists update_stars on review;

create trigger update_stars
    after insert on review
    for each row
execute function update_total_average_count_stars();

create or replace function update_total_checkins()
returns trigger as $update_business_checkins$
    begin
        update business
        set num_checkins = num_checkins + new.total_checkins
        from checkin_day
        where business.business_id = checkin_day.fk_business_id
        and checkin_day.day_id = new.fk_day_id;
        return new;
    end;
$update_business_checkins$ language plpgsql;

create trigger update_num_checkins
    after insert on checkin_hour
    for each row
execute function update_total_checkins();