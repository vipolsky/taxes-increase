queries_dict = {
    "LTV":
        """
with
first_observed_stay as (
    select requester_id
         , case
            when service_type like 'over%' then 'overnight'
            when service_type = 'drop-in' or service_type = 'over' then 'drop-in'
            when service_type like '%walk%' then 'dog-walking'
        end as service
        , max(case when b.stay_id = first_stay_id then 1 else 0 end) as is_new
        , min(stay_added) as stay_added
        , case
            when max(case when b.stay_id = first_stay_id then 1 end) = 1 and p.added is not null then 1 else 0
        end as new_account
    from standard_reports.bookings b
             join standard_reports.bookings_financial f
                  on f.stay_id = b.stay_id and f.owner_fee_percentile = 0.07
             join standard_reports.bookings_location l
                  on l.stay_id = b.stay_id
                      and l.provider_country = 'US'
             left join roverdb.people_person p
                       on p.id = requester_id
                           and datediff(day, added, getdate()) <= 389
                           and datediff(day, added, getdate()) >= 360
    where datediff(day, stay_added, getdate()) <= 389
      and datediff(day, stay_added, getdate()) >= 360
      and service_type not like 'on-%'
      and service is not null
    group by 1,2, p.added

  union all

    select
        requester_id
        ,'*' as service
        , max(case when b.stay_id = first_stay_id then 1 else 0 end) as is_new
        , min(stay_added) as stay_added
        , case
            when max(case when b.stay_id = first_stay_id then 1 end) = 1 and p.added is not null then 1 else 0
        end as new_account
    from standard_reports.bookings b
        join standard_reports.bookings_financial f
            on f.stay_id=b.stay_id and f.owner_fee_percentile=0.07
        join standard_reports.bookings_location l
            on l.stay_id=b.stay_id
            and l.provider_country='US'
        left join roverdb.people_person p
            on p.id = requester_id
            and datediff(day, added, getdate()) <= 389
            and datediff(day, added, getdate()) >= 360
      where datediff(day,stay_added,getdate())<=389
        and datediff(day,stay_added,getdate())>=360
        and service_type not like 'on-%'
    group by requester_id, p.added
)
, later_stays as (
    select
        b1.requester_id
        , b1.service
        , case when b1.is_new = 1 then 'new' else 'repeat' end as new_repeat
        , b1.new_account
        , b1.stay_added as first_observed_stay_added
        , dates
        , count(distinct b2.stay_id) as stays
        , coalesce(sum(bf.rover_take),0) as nrt
        , coalesce(sum(bf.services_total),0) as gmv
    from first_observed_stay b1
        join (
            select 30 as dates union all
            select 180 as dates union all
            select 360 as dates
        ) on true
        left join roverdb.conversations_conversation c on true
            and c.requester_id = b1.requester_id
            and c.added > b1.stay_added
        left join standard_reports.bookings b2 on true
            and b2.conversation_id = c.id
            and b2.stay_added > b1.stay_added
            and datediff(day,b1.stay_added,b2.stay_added)<=dates
            and b2.service_type not like 'on-%'
  left join standard_reports.bookings_financial bf using (stay_id)
  group by b1.requester_id, b1.service, b1.is_new, b1.new_account, b1.stay_added, dates
)
select *
from later_stays
            """
    , "retrans":
"""
with
    needs as (
        select
            case
                when first_touch_service_type like 'over%' then 'overnight'
                when first_touch_service_type like 'drop%' then 'drop-in'
                when first_touch_service_type like '%walk%' then 'dog-walking'
            end as service
            , case
                when requester_was_new_customer=1 then 'new' else 'repeat'
            end as new_repeat
            , case
                when requester_was_new_customer=1 and p.added is not null then 'new' else 'old'
            end as new_account
            , count(n.need_id) as num_needs
            , sum(booked::int) as num_booked
            , num_booked*1.0/num_needs as ntb14
        from standard_reports.needs n
            join roverdb.conversations_conversation c
                on first_touch_conversation_id = c.id
                and owner_fee_percentile=0.07
            join standard_reports.need_to_book ntb
                on n.need_id = ntb.need_id
                and lag=14
            left join roverdb.people_person p
                on p.id = n.requester_id
                and datediff(day,convert_timezone('pst', p.added),convert_timezone('pst',getdate())) <= 45
                and datediff(day,convert_timezone('pst', p.added),convert_timezone('pst',getdate())) > 15
        where true
            and country='US'
            and datediff(day,convert_timezone('pst',need_added),convert_timezone('pst',getdate())) <= 45
            and datediff(day,convert_timezone('pst',need_added),convert_timezone('pst',getdate())) > 15
            and first_touch_service_type not like 'on-%'
            and service is not null
        group by 1,2,3

      union all

        select
            '*' as service
            , case
                when requester_was_new_customer=1 then 'new' else 'repeat'
            end as new_repeat
            , case
                when requester_was_new_customer=1 and p.added is not null then 'new' else 'old'
            end as new_account
            , count(n.need_id) as num_needs
            , sum(booked::int) as num_booked
            , num_booked*1.0/num_needs as ntb14
        from standard_reports.needs n
            join roverdb.conversations_conversation c
                on first_touch_conversation_id = c.id
                and owner_fee_percentile=0.07
            join standard_reports.need_to_book ntb
                on n.need_id = ntb.need_id
                and lag=14
            left join roverdb.people_person p
                on p.id = n.requester_id
                and datediff(day,convert_timezone('pst', p.added),convert_timezone('pst',getdate())) <= 45
                and datediff(day,convert_timezone('pst', p.added),convert_timezone('pst',getdate())) > 15
        where true
            and country='US'
            and datediff(day,convert_timezone('pst',need_added),convert_timezone('pst',getdate())) <= 45
            and datediff(day,convert_timezone('pst',need_added),convert_timezone('pst',getdate())) > 15
            and first_touch_service_type not like 'on-%'
            and service is not null
        group by 1,2,3
)
, first_observed_stay as (
    select requester_id
         , case
            when service_type like 'over%' then 'overnight'
            when service_type = 'drop-in' or service_type = 'over' then 'drop-in'
            when service_type like '%walk%' then 'dog-walking'
        end as service
        , max(case when b.stay_id = first_stay_id then 'new' else 'repeat' end) as new_repeat
        , min(stay_added) as stay_added
        , case
            when max(case when b.stay_id = first_stay_id then 1 end) = 1 and p.added is not null then 'new' else 'old'
        end as new_account
    from standard_reports.bookings b
             join standard_reports.bookings_financial f
                  on f.stay_id = b.stay_id and f.owner_fee_percentile = 0.07
             join standard_reports.bookings_location l
                  on l.stay_id = b.stay_id
                      and l.provider_country = 'US'
             left join roverdb.people_person p
                       on p.id = requester_id
                           and datediff(day, added, getdate()) <= 389
                           and datediff(day, added, getdate()) >= 360
    where datediff(day, stay_added, getdate()) <= 389
      and datediff(day, stay_added, getdate()) >= 360
      and service_type not like 'on-%'
      and service is not null
    group by 1,2, p.added

  union all

    select
        requester_id
        ,'*' as service
        , max(case when b.stay_id = first_stay_id then 'new' else 'repeat' end) as new_repeat
        , min(stay_added) as stay_added
        , case
            when max(case when b.stay_id = first_stay_id then 1 end) = 1 and p.added is not null then 'new' else 'old'
        end as new_account
    from standard_reports.bookings b
        join standard_reports.bookings_financial f
            on f.stay_id=b.stay_id and f.owner_fee_percentile=0.07
        join standard_reports.bookings_location l
            on l.stay_id=b.stay_id
            and l.provider_country='US'
        left join roverdb.people_person p
            on p.id = requester_id
            and datediff(day, added, getdate()) <= 389
            and datediff(day, added, getdate()) >= 360
      where datediff(day,stay_added,getdate())<=389
        and datediff(day,stay_added,getdate())>=360
        and service_type not like 'on-%'
    group by requester_id, p.added
    )
, retrans as (
    select
        b1.requester_id
        , b1.service
        , b1.new_repeat
        , b1.new_account
        , max((c.requester_id is not null)::int) as reneeded14
        , max((b2.requester_id is not null)::int) as rebooked14
    from first_observed_stay b1
        left join roverdb.conversations_conversation c on true
            and c.requester_id = b1.requester_id
            and c.added > b1.stay_added
            and c.added < dateadd(day,14,b1.stay_added)
        left join standard_reports.bookings b2 on true
            and b2.conversation_id = c.id
            and b2.stay_added > b1.stay_added
            and b2.stay_added < dateadd(day,14,b1.stay_added)    --and b2.service_type not like 'on-%'
    group by 1,2,3,4
)
, requesters as (
    select service
    , new_repeat
    , new_account
    , count(requester_id) as num_owners
    , sum(reneeded14) as reneeded14
    , sum(rebooked14) as rebooked14
    from retrans
    group by 1,2,3
)
select *
from needs
join requesters using(new_repeat, new_account, service)
"""
}