
--requester started a conversation during bucketing period (model off data from one year ago)
--requester must have never booked before at the time the earliest conversation (in the booking period) was started
--requester must be in the US
--requester has owner side fees "turned on"
with eligible_requesters as (
    select
        c.requester_id
        , min(c.added) as bucketing_date
    from
         roverdb.conversations_conversation c
    left join
        standard_reports.b_stays s
            on s.requester_id = c.requester_id
                and s.new_to_rover = 1
                and c.added > s.stay_added --conversation was added after first booking occurred (customer was not new at time of conversation)
    join
        standard_reports.needs n
            on n.need_id = c.need_id
                and n.country = 'US'
    where
        c.added between dateadd(day, -365, getdate()) and dateadd(day, -(365-30), getdate())
            and s.requester_id is null --only requesters who did not book a stay before the start of the conversation
            and c.owner_fee_percentile is not null
    group by c.requester_id
)
, conversations as (
    select
        r.requester_id
        , r.bucketing_date
        , c.added as conversation_added
        , c.id as conversation_id
    from eligible_requesters r
    join roverdb.conversations_conversation c
        on c.requester_id = r.requester_id
            and c.added between dateadd(day, -365, getdate()) and dateadd(day, -180, getdate())
            --and datediff(day, r.bucketing_date, c.added) <= 5
            and c.owner_fee_percentile is not null
    join standard_reports.needs n
        on n.need_id = c.need_id
            and n.country = 'US'
)
, lag_180_ltv as (
    select c.requester_id
         , c.bucketing_date
         , sum(coalesce(bf.services_total, 0.0)) as gmv
    from conversations c
             left join standard_reports.b_stays s
                       on c.conversation_id = s.conversation_id
                           and s.stay_added between dateadd(day, -365, getdate()) and dateadd(day, -180, getdate())
             left join standard_reports.bookings_financial bf
                       on bf.stay_id = s.stay_id
    group by c.requester_id, c.bucketing_date
)
select * from lag_180_ltv
   ;

