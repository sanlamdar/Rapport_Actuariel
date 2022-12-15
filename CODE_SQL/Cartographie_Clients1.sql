
--create table actuary.cartograph1 as (
 select
 
   i.ANNEE, 
 -- deal_date view_date,
 '20220930' view_date,
 --i.GESTIONNAIRE,
  i.CODEBRAN,k.LIBEBRAN,k.LIBEBRAN1,k.LIBEBRAN2,
 case when j.codecate is null then m.codecate ELSE j.codecate  end codecate,
  case when j.RAISOCIN is null then m.RAISOCIN ELSE j.RAISOCIN  end RAISOCIN,
  case when j.RAISOCINB is null then m.RAISOCINB ELSE j.RAISOCINB  end RAISOCINB,
  j.LIBTYPIN LIBTYPIN,
  i.CODEINTE,
  case when j.CODEINTEB is null then m.CODEINTEB ELSE j.CODEINTEB  end CODEINTEB,
  case when j.CODEASSU is null then m.CODEASSU ELSE j.CODEASSU  end CODEASSU,
  case when j.CODEASSUB is null then m.CODEASSUB ELSE j.CODEASSUB  end CODEASSUB,
  case when j.NOM_ASSU is null then m.NOM_ASSU ELSE j.NOM_ASSU  end NOM_ASSU,
  case when j.NOMB is null then m.NOMB ELSE j.NOMB  end NOM_ASSUB,
  case when j.GENRASSU is null then m.GENRASSU ELSE j.GENRASSU  end GENRASSU,
  case when j.GENRASSUB is null then m.GENRASSUB ELSE j.GENRASSUB  end GENRASSUB,
  
  i.NUMEPOLI,
    nvl(PRIME_EMISE,0)              PRIME_EMISE          ,
   nvl(PRIMNETT_EMISSION        ,0) PRIMNETT_EMISE      ,
   nvl(PARC_RISQUE              ,0) PARC_RISQUE          ,
   nvl(NB_AVENANT               ,0) NB_AVENANT           ,
   nvl(ACCEQUIT_EMISE           ,0) ACCEQUIT_EMISE       ,
   nvl(COMISSION_EMISE          ,0) COMISSION_EMISE      ,
   nvl(PRIME_EFFET              ,0) PRIME_EFFET          ,
   nvl(PRIMNETT_EFFET           ,0) PRIMNETT_EFFET       ,
   nvl(ACCEQUIT_EFFET           ,0) ACCEQUIT_EFFET       ,
   nvl(COMISSION_EFFET          ,0) COMISSION_EFFET      ,
   nvl(EXPOSITION               ,0) EXPOSITION           ,
   nvl(PRIME_ACQUISE            ,0) PRIME_ACQUISE        ,
   nvl(PRIMNETT_ACQUISE         ,0) PRIMNETT_ACQUISE     ,
   nvl(SINPAY                   ,0) SINPAY               ,
   nvl(SINPAY_ANT               ,0) SINPAY_ANT           ,
   nvl(RECENC                   ,0) RECENC               ,
   nvl(RECENC_ANT               ,0) RECENC_ANT           ,
   nvl(SAP                      ,0) SAP                  ,
   nvl(AREC                     ,0) AREC                 ,
   nvl(CHARGE_SIN_SURVENANCE    ,0) CHARGE_SIN_SURVENANCE ,
   nvl(NBSIN_SURVENANCE         ,0) NBSIN_SURVENANCE     ,
    nvl(CHARGE_SIN_SURVENANCE_GRAVE    ,0) CHARGE_SIN_SURVENANCE_GRAVE ,
   nvl(NBSIN_SURVENANCE_GRAVE        ,0) NBSIN_SURVENANCE_GRAVE     ,
   nvl(CHARGE_SIN_EFFET         ,0) CHARGE_SIN_EFFET     ,
   nvl(NBSIN_EFFET              ,0) NBSIN_EFFET          ,
   nvl(CHARGE_DTSOINS           ,0) CHARGE_DTSOINS       ,
   nvl(CHARGE_COMPTABLE         ,0) CHARGE_COMPTABLE    -- ,
   -- nvl(CHARGE_DTSOINS_GEST           ,0) CHARGE_DTSOINS_GEST       ,
   --nvl(CHARGE_COMPTABLE_GEST         ,0) CHARGE_COMPTABLE_GEST     
  
  
  from
 (select* from actuary.dtm_actuary1)i,
 (
 select
 --ANNEE,
 i.CODEINTE,CODEINTEB, NUMEPOLI, LIBTYPIN,i.RAISOCIN,RAISOCINB, i.CODEASSU,k.CODEASSUB,i.NOM_ASSU,NOMB,k.GENRASSU,k.GENRASSUB, CODECATE
 
 from (
 select -- to_char(datecomp,'yyyy') ANNEE, 
 to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI,
    max(LIBTYPIN) LIBTYPIN,
    max(RAISOCIN) RAISOCIN,
    max(CODEASSU) CODEASSU,
    max(NOM_ASSU) NOM_ASSU,
    max(CODECATE) CODECATE
from actuary.TABLE_PRIME
group by to_char(CODEINTE), to_char(NUMEPOLI)
--,to_char(datecomp,'yyyy')
)i,
(select*from actuary.tb_assu)k,
(select to_char(codeinte) codeinte,RAISOCIN,codeinteb,raisocinb from actuary.tb_inter)l

where i.CODEASSU=k.CODEASSU (+) and  i.CODEINTE=l.CODEINTE (+)


) j,


 (
  select
 --ANNEE, 
 i.CODEINTE,CODEINTEB, NUMEPOLI,i.RAISOCIN,RAISOCINB, i.CODEASSU,CODEASSUB,i.NOM_ASSU,NOMB,K.GENRASSU,K.GENRASSUB, CODECATE
 
 from (
 
 select --to_char(ANNEE) ANNEE, 
 to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI,--DATEEFFE,DATEECHE,
--CODEINTE,

   -- max(LIBTYPIN) LIBTYPIN,
    max(RAISOCIN) RAISOCIN,
    max(CODEASSU) CODEASSU,
    max(NOM) NOM_ASSU,
    max(CODECATE) CODECATE


from actuary.CHARGE_SINISTRE

group by to_char(CODEINTE),to_char(NUMEPOLI) 
--,ANNEE
)i,

(select*from actuary.tb_assu)k,
(select to_char(codeinte) codeinte,RAISOCIN,codeinteb,raisocinb from actuary.tb_inter)l

where i.CODEASSU=k.CODEASSU (+) and  i.CODEINTE=l.CODEINTE (+))m,


(select CODEBRAN, LIBSYS LIBEBRAN, LIB1 LIBEBRAN1, LIB2 LIBEBRAN2 from actuary.branche)k

where
    i.CODEBRAN=k.CODEBRAN(+)
---and i.ANNEE=J.ANNEE(+)
and i.CODEINTE=j.CODEINTE(+)
and i.NUMEPOLI=j.NUMEPOLI(+)

--and i.ANNEE=m.ANNEE(+)
and i.CODEINTE=m.CODEINTE(+)
and i.NUMEPOLI=m.NUMEPOLI(+)

--)
;

-------------------------------------------------------------------
--count(distinct CODEASSU2021)-1 NB_CLIENTS2021, count(distinct CODEASSU2022)-1 NB_CLIENTS2022,count(distinct CODEASSU) NB_CLIENTS_ASSAINIS,  count(distinct CODEINTE||'_'||NUMEPOLI) NB_CONTRATS

select CODEBRAN,nvl(GENRASSUB,'PP') GENRASSUB,count(distinct CODEASSU2021)-1 NB_CLIENTS2021, count(distinct CODEASSU2022)-1 NB_CLIENTS2022,
count(distinct CODEASSUB2021)-1 NB_CLIENTSB2021, count(distinct CODEASSUB2022)-1 NB_CLIENTSB2022,  
count(distinct CODCONTRAT2021)-1 NB_CONTRATS2021,count(distinct CODCONTRAT2022)-1 NB_CONTRATS2022,sum(nvl(EXPOSITION2021,0)) EXPOSITION2021,
sum(nvl(EXPOSITION2022,0)) EXPOSITION2022, sum(CHIFFAFA2021) CHIFFAFA2021,sum(CHIFFAFA2022) CHIFFAFA2022,sum(CHIFFAFA2022)/decode(sum(nvl(EXPOSITION2022,0)),0,count(distinct CODEINTE||'_'||NUMEPOLI),
sum(nvl(EXPOSITION2022,0))) PMOY from

  (select
        i.ANNEE                                                                       ,
     -- '20220430'   VIEW_DATE                                                                     ,
      VIEW_DATE ,
       CODEBRAN                                                                      ,
        LIBEBRAN                                                                      ,
        LIBEBRAN1                                                                     ,
        LIBEBRAN2                                                                     ,
        CODECATE                                                                      ,
        case when RAISOCIN is null then  RAISOCINg else RAISOCIN end RAISOCIN         ,
        case when RAISOCINB is null then  RAISOCINBg else RAISOCINB end RAISOCIN_ASSAINI     ,
        case when LIBTYPIN is null then  LIBTYPINg else LIBTYPIN end LIBTYPIN         ,
        --nvl(GESTIONNAIRE,4444) CODEGEST,
        --case when GESTIONNAIRE=3003 then 'WTW' 
        --     when GESTIONNAIRE=3002 then 'ASCOMA' 
        --      when GESTIONNAIRE=3918 then 'OLEA' 
        --       when GESTIONNAIRE=0 then 'MCI' else 'AUCUN' end LIBEGEST,
        i.CODEINTE                                                                    ,
         case when CODEINTEB is null then  CODEINTEBg else CODEINTEB end CODEINTEB         ,
        CODEASSU                                                                      ,
        CODEASSUB                                                                     ,
        NOM_ASSU                                                                      ,
        NOM_ASSUB   NOM_ASSU_ASSAINI, 
        GENRASSU,
        GENRASSUB                                                                 ,
        NUMEPOLI                                                                      ,
        --PRIME_EMISE                                                                   ,
        PRIMNETT_EMISE                                                                ,
        PARC_RISQUE                                                                   ,
        NB_AVENANT                                                                    ,
        ACCEQUIT_EMISE                                                                ,
        COMISSION_EMISE                                                               ,
        PRIME_EFFET                                                                   ,
        PRIMNETT_EFFET                                                                ,
        ACCEQUIT_EFFET                                                                ,
        COMISSION_EFFET                                                               ,
        --EXPOSITION                                                                    ,
        PRIME_ACQUISE                                                                 ,
        PRIMNETT_ACQUISE                                                              ,
        SINPAY                                                                        ,
        SINPAY_ANT                                                                    ,
        RECENC                                                                        ,
        RECENC_ANT                                                                    ,
        SAP                                                                           ,
        AREC                    ,
        CHARGE_SIN_SURVENANCE CHARGE_SIN_MILLIARD ,
        case when CODEBRAN=81 then CHARGE_DTSOINS else CHARGE_SIN_SURVENANCE  end CHARGE_SIN_SURVENANCE                                                         ,
        case when CODEBRAN=81 then 0 else  NBSIN_SURVENANCE   end  NBSIN_SURVENANCE                                                          ,
         CHARGE_SIN_SURVENANCE_GRAVE ,
   NBSIN_SURVENANCE_GRAVE   ,
        CHARGE_SIN_EFFET                                                              ,
        NBSIN_EFFET                                                                   ,
        CHARGE_DTSOINS                                                                ,
        --decode(i.ANNEE,'2021',i.CODEASSU,0) CODEASSU2021,
        --decode(i.ANNEE,'2021',i.CODEASSU,0) CODEASSU2021,
        --decode(i.ANNEE,'2021',I.CODEINTE||'_'||I.NUMEPOLI,0) CODCONTRAT2021,
        --decode(i.ANNEE,'2022',I.CODEINTE||'_'||I.NUMEPOLI,0) CODCONTRAT2022,
        case when i.annee=2021 and i.PRIME_EMISE<>0 then I.CODEINTE||'_'||I.NUMEPOLI else '_' end CODCONTRAT2021,
        case when i.annee=2022 and i.PRIME_EMISE<>0 then I.CODEINTE||'_'||I.NUMEPOLI else '_' end CODCONTRAT2022,
        
        case when i.annee=2021 and i.PRIME_EMISE<>0 then i.codeassu else 0 end CODEASSU2021,
        case when i.annee=2022 and i.PRIME_EMISE<>0 then i.codeassu else 0 end CODEASSU2022,
        case when i.annee=2021 and i.PRIME_EMISE<>0 then i.codeassub else 0 end CODEASSUB2021,
        case when i.annee=2022 and i.PRIME_EMISE<>0 then i.codeassub else 0 end CODEASSUB2022,
        --decode(i.ANNEE,'2021',i.CODEASSU,0) CODEASSU2021,
        --decode(i.ANNEE,'2022',i.CODEASSU,0) CODEASSU2022,
        decode(i.ANNEE,'2021',i.EXPOSITION,0) EXPOSITION2021,
        decode(i.ANNEE,'2022',i.EXPOSITION,0) EXPOSITION2022,
        decode(i.ANNEE,'2021',i.PRIME_EMISE,0) CHIFFAFA2021,
        decode(i.ANNEE,'2022',i.PRIME_EMISE,0) CHIFFAFA2022,
        CHARGE_COMPTABLE                                                             -- ,
       -- CHARGE_DTSOINS_GEST                                                           ,
       -- CHARGE_COMPTABLE_GEST

  from 
  (select * from actuary.cartograph1)i,
  (select 
   ANNEE,CODEINTE,
    max(CODEINTEB) CODEINTEBg,
    max(RAISOCIN) RAISOCINg,
   	max(RAISOCINB) RAISOCINBg,
   	max(LIBTYPIN) LIBTYPINg

  from actuary.cartograph1 group by ANNEE,CODEINTE)j
where 
    i.CODEINTE=j.CODEINTE(+)
and i.ANNEE=j.ANNEE(+)
and i.annee in (2021,2022)
--and I.PRIME_EMISE <> 0
  )
  where codebran not in (6,61)
  group by CODEBRAN,nvl(GENRASSUB,'PP')
--)
;

select * from ACTUARY.PN_RISQUE1;


select count(*) from 
(select t.*,GENRASSU from ACTUARY.PN_RISQUE1 t,
assure j
where T.CODEASSU=J.CODEASSU)
where genrassu is null;

select * from 
(select t.*,GENRASSU from ACTUARY.PN_RISQUE1 t,
assure j
where T.CODEASSU=J.CODEASSU)
where codeassu=-143240;


select * from 
(select t.*,GENRASSU from ACTUARY.PN_RISQUE1 t,
assure j
where T.CODEASSU=J.CODEASSU)
where regexp_like(nom_assu, '^(SEMEG|OLE)');
