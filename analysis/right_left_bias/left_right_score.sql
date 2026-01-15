from '/Users/lorenzkort/Documents/LocalCode/news-data/data/seeds/pew_2017.csv'
select   round(avg(case when Q10NLa > 90 then null else Q10NLa end), 3) as NOS,
  round(avg(case when Q10NLb > 90 then null else Q10NLb end), 3) as RTL,
  round(avg(case when Q10NLc > 90 then null else Q10NLc end), 3) as AD,
  round(avg(case when Q10NLd > 90 then null else Q10NLd end), 3) as VK,
  round(avg(case when Q10NLe > 90 then null else Q10NLe end), 3) as TG,
  round(avg(case when Q10NLf > 90 then null else Q10NLf end), 3) as NU,
  round(avg(case when Q10NLg > 90 then null else Q10NLg end), 3) as GS,
  round(avg(case when Q10NLh > 90 then null else Q10NLh end), 3) as JOOP,
  round(avg(case when Q10NLi > 90 then null else Q10NLi end), 3) as Q10NLi
where Q10NLa is not null

-- 3.346	3.674	3.366	2.651	4.056	3.355	3.88	2.732	3.568