with articles as (
  select *
  from "/Users/lorenzkort/Documents/LocalCode/partisan-news2019/dpgMedia2019-articles-byarticle-all.jsonl"
)
, labels as (
  select *
  from "/Users/lorenzkort/Documents/LocalCode/partisan-news2019/annotations.csv"
)
, publishers as (
  select *
  from "/Users/lorenzkort/Documents/LocalCode/partisan-news2019/dpgMedia2019-labels-bypublisher.jsonl"
)
, merged as (
  select publisher, Partisanship, lower(Polarity) Polarity, "Pro-", "Anti-", title, 
  from labels l
  left join articles a on a.Id = l.ExternalId
  left join publishers p on p.Id = l.ExternalId
)
, metalabel as (
  from merged
  select
    publisher, 
    case when Polarity like '%links%' then -1
      when Polarity like '%rechts' then 1
      else null end as links_rechts,
    case when Polarity like '%progressief%' then -1
      when Polarity like '%conservatief%' then 1
      else null end as conv_prog,
    where publisher is not null
)
from metalabel
select 
  publisher, 
  round(sum(links_rechts) / sum(abs(links_rechts)), 1) lr_perc,
  round(sum(conv_prog) / sum(abs(conv_prog)), 1) pc_perc, 
  count(*) articles
group by publisher