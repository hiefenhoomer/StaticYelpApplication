
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