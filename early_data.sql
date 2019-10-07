--requester started a conversation during bucketing period (model off 30 days shifted by 7)
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
        c.added between dateadd(day, -37, getdate()) and dateadd(day, -7, getdate())
            and s.requester_id is null --only requesters who did not book a stay before the start of the conversation
            and c.owner_fee_percentile is not null
    group by c.requester_id
)
, early_conversations as (
    select
        r.requester_id
        , r.bucketing_date
        , c.added as conversation_added
        , c.id as conversation_id
    from eligible_requesters r
    join roverdb.conversations_conversation c
        on c.requester_id = r.requester_id
            and c.added between dateadd(day, -37, getdate()) and dateadd(day, -2, getdate())
            and datediff(day, r.bucketing_date, c.added) <= 5
            and c.owner_fee_percentile is not null
    join standard_reports.needs n
        on n.need_id = c.need_id
            and n.country = 'US'
)
, lag_5_ltv as (
    select ec.requester_id
         , ec.bucketing_date
         , sum(coalesce(bf.services_total, 0.0)) as gmv
    from early_conversations ec
             left join standard_reports.b_stays s
                       on ec.conversation_id = s.conversation_id
                           and datediff(day, ec.bucketing_date, s.stay_added) <= 5
             left join standard_reports.bookings_financial bf
                       on bf.stay_id = s.stay_id
    group by ec.requester_id, ec.bucketing_date
)
select * from lag_5_ltv
;