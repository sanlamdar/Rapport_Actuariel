CREATE OR REPLACE PROCEDURE ACTUARY.proc_RE(month_id in varCHAR2,month_id_pred in varCHAR2, itt in int, fin in int, deal_date in varCHAR2) as

begin

delete from  ACTUARY.TABLE_PRIME 
--where TO_CHAR (DATECOMP,'yyyymm')=month_id
;

 --drop table    ACTUARY.TABLE_PRIME
 --create table ACTUARY.TABLE_PRIME as (
insert into ACTUARY.TABLE_PRIME
select 
   LIBTYPIN,
   CODEINTE,
   RAISOCIN,
   CODEASSU,
   NOM_ASSU,
   CODEBRAN,
   LIBEBRAN,
   CODECATE,
   LIBECATE,
   NUMEPOLI,
  --CODERISQ,
  --CODEGARA,
   NVL(NUMEAVEN,0) NUMEAVEN,
  -- NUMEAVEN_G,
   DATEEFFE,
   DATEECHE,
   DATECOMP,
   sum( primnett ) PRIMNETT,
  sum( PRINETCO ) PRIMNETTCO,
  sum( CHIFAFFA) CHIFAFFA,
  sum( ACCEQUIT )ACCEQUIT,
   sum(COMMISSI )COMMISSI
      
 from 
(select* 
 from  orass_v6.v_ch_affaire
where TO_CHAR (DATECOMP,'yyyy')>='1990'
  -- and codebran=4
 and  codeinte  not in (9999,9998,9995) and genrmouv<>'C')

group by 

   LIBTYPIN,
   CODEINTE,
   RAISOCIN,
   CODEASSU,
   NOM_ASSU,
   CODEBRAN,
   LIBEBRAN,
   CODECATE,
   LIBECATE,
   NUMEPOLI,
   NVL(NUMEAVEN,0) ,
   DATEEFFE,
   DATEECHE,
   DATECOMP
 
;

delete from  ACTUARY.charge_sinistre ;

insert into ACTUARY.charge_sinistre 

select  to_char(x.datesurv,'yyyy') Annee,
     month_id deal_date,
        x.codeinte ,
       i.raisocin ,
       x.numesini ,
       x.refeinte ,
       x.NUMEPOLI ,
       x.NUMEAVEN,
       decode(k.dateeffe,NULL,decode(l.dateeffe, NULL,m.dateeffe,l.dateeffe),k.dateeffe) dateeffe,
       decode(k.dateeche,NULL,decode(l.dateeche, NULL,m.dateeche,l.dateeche),k.dateeche) dateeche,   
       
       --decode(k.dateeffe,NULL,l.dateeffe, k.dateeffe) dateeffe,
       --decode(k.dateeche,NULL,l.dateeche,k.dateeche) dateeche,   
       
       x.codeassu ,
       x.nom_assu nom,
       c.codebran , B.LIBEBRAN,
       x.CODECATE  ,
       x.caterisq  ,
       substr(c.libecate,1, 40) libecate,
        x.codegara,x.CODERISQ,
       decode(x.natusini,'S','Maladie','M','Materiel','C','Corporel','D','Mat et Corporel')  natusini,
       g.libegara  ,
       x.datesurv  ,
       x.datedecl   ,
--      x.dateeval Date eval/reglt/Recours,
        sum(sinpay) sinpay,
        sum(sinpay_ant)sinpay_ant,   
        sum(recenc) recenc,
        sum(recenc_ant) recenc_ant,  
        sum(sinpay) - sum(recenc) Solde_paiements,
        sum(eval) eval,
        sum(sap) sap,  
        sum(arec) arec , 
       
(sum(sap) + sum(sinpay_ant)+sum(sinpay)) - (sum(arec)+ sum(recenc_ant)+ sum(recenc)) Couts_sinistres 
       ,x.CODTYPSO 
       ,x.LIBTYPSO
     
  from
(
         --************  Reglements sinistres  et recours encaissé de la periode    **********************---------------
select m.typemouvement,
       m.codeinte,
       m.numesini,
       m.nom_assu,
       m.codeassu,
       m.codecate,
       m.codegara,
       m.caterisq,S.CODERISQ,
       m.datesurv,
       m.datedecl,
       m.dateeval,
       m.refeinte,
       m.NUMEPOLI,
       nvl(m.NUMEAVEN,0)  NUMEAVEN,
       m.natusini, 
       sum(decode(m.typemouvement,'REGLE',-m.monteval,0)) sinpay,
       0 sinpay_ant, 
       sum(decode(m.typemouvement,'RENC', -m.monteval,0)) recenc,
       0 recenc_ant, 
	   0 Eval,
       0 sap, 
       0 arec ,ss.CODTYPSO ,ts.LIBTYPSO 
   from orass_v6.v_mouvement_sinistre m , orass_v6.sort_sinistre ss, orass_v6.type_sort ts ,orass_v6.sinistre s  
  -- to_char(trunc(ss2.datsorsi),'YYYYMM')
   where to_char(trunc(m.dateeval),'YYYYMM') between substr(month_id,1,4)||'01'  and month_id
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
   and m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
     and ss.datsorsi = (select max(ss2.datsorsi)
                        from orass_v6.sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                         and to_char(trunc(ss2.datsorsi),'YYYYMM')  <= month_id)
   and ss.codtypso = ts.codtypso
  -- and ts.natusort in ('OU','RO')   
--  and  m.codeinte=3044 and m.numesini=500232 and to_char(m.datesurv,'yyyy')=2017  
--and  m.codeinte=3023 and m.numesini=300278 and to_char(M.datesurv,'yyyy')=2016
 
 --and  m.codeinte=1021 and m.numesini=8214 and to_char(m.datesurv,'yyyy')=1990
 group by m.typemouvement,ss.CODTYPSO ,ts.LIBTYPSO,m.natusini,m.dateeval,m.codecate,m.refeinte,m.caterisq,S.CODERISQ,m.codeinte,m.numesini,m.nom_assu,m.codeassu,m.datesurv,m.datedecl,m.codegara ,m.NUMEPOLI, nvl(m.NUMEAVEN,0) 
union all
    --************  Reglements sinistres  et recours encaissé de la periode passée jusqu'a l'origine du sinistres    **********************---------------
select m.typemouvement,
       m.codeinte,
       m.numesini,
       m.nom_assu,
       m.codeassu,
       m.codecate,
        m.codegara,
       m.caterisq,S.CODERISQ,
       m.datesurv,
       m.datedecl,
       m.dateeval,
       m.refeinte,
       m.NUMEPOLI,
       nvl(m.NUMEAVEN,0)  NUMEAVEN,
       m.natusini, 
       0 sinpay,
       sum(decode(m.typemouvement,'REGLE',-m.monteval,0)) sinpay_ant,
       
       0 recenc,
       sum(decode(m.typemouvement,'RENC', -m.monteval,0)) recenc_ant,
       0 Eval,
       0 sap, 
       0 arec,ss.CODTYPSO ,ts.LIBTYPSO
       
  from orass_v6.v_mouvement_sinistre m , orass_v6.sort_sinistre ss, orass_v6.type_sort ts,orass_v6.sinistre s
 where to_char(trunc(m.dateeval),'YYYYMM') <=month_id_pred
 and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
    and m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from orass_v6.sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                         and to_char(trunc(ss2.datsorsi),'YYYYMM') <= month_id)
   and ss.codtypso = ts.codtypso
    --and ts.natusort in ('OU','RO')          
 
 group by m.typemouvement,ss.CODTYPSO ,ts.LIBTYPSO,m.natusini,m.dateeval,m.codecate,m.refeinte,m.caterisq,S.CODERISQ,m.codeinte,m.numesini,m.nom_assu,m.codeassu,m.datesurv,m.datedecl,m.codegara ,m.NUMEPOLI, nvl(m.NUMEAVEN,0) 

union all

------------------------------********************* Eval total et SAP et recours a encaisser *********************-------------
select m.typemouvement,
       m.codeinte,
       m.numesini,
       m.nom_assu,
       m.codeassu,
       m.codecate,
        m.codegara,
       m.caterisq,
       S.CODERISQ,
       m.datesurv,
       m.datedecl,
       m.dateeval,
       m.refeinte,
       m.NUMEPOLI,
       nvl(m.NUMEAVEN,0)  NUMEAVEN,
       m.natusini, 
       0 sinpay,
       0 sinpay_ant, 
	   
       0 recenc, 
       0 recenc_ant,
       sum(decode(m.typemouvement,'EVAL', m.monteval,0)) eval,
       sum(decode(m.typemouvement,'EVAL', m.monteval,0)) + sum(decode(m.typemouvement,'REGLE',m.monteval,0)) sap,
        
       sum(decode(m.typemouvement,'ESTR', m.monteval,0)) + sum(decode(m.typemouvement,'RENC', m.monteval,0)) arec 
	   ,ss.CODTYPSO ,ts.LIBTYPSO
       
  from orass_v6.v_mouvement_sinistre m, orass_v6.sort_sinistre ss, orass_v6.type_sort ts,orass_v6.sinistre s
 where to_char(trunc(m.dateeval),'YYYYMM') <= month_id
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
    and m.codeinte = s.codeinte
   and m.exersini  = s.exersini
   and m.numesini  = s.numesini
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from orass_v6.sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                         and to_char(trunc(ss2.datsorsi),'YYYYMM') <= month_id)
   and ss.codtypso = ts.codtypso
   and ts.natusort in ('OU','RO')
 group by  m.NUMEPOLI, nvl(m.NUMEAVEN,0), m.typemouvement,ss.CODTYPSO ,ts.LIBTYPSO,m.natusini,m.dateeval,m.codecate, m.codegara ,m.refeinte, m.caterisq,S.CODERISQ,m.codeinte,m.numesini,m.nom_assu,m.codeassu,m.datesurv,m.datedecl

) x, categorie c,intermediaire i  ,reference_garantie g  ,(select*from orass_v6.branche) b , 

(select numepoli,codeinte, nvl(numeaven,0) numeaven, dateffav dateeffe, datechav dateeche from orass_v6.avenant) k,
(select numepoli,codeinte, nvl(numeaven,0) numeaven,  min(dateeffe) dateeffe,  min(dateeche) dateeche
 from orass_v6.v_ch_affaire where typemouv='EMISSIONS'  and genrmouv<>'C' --and nvl(numeaven,0)=0
 group by numepoli,codeinte, nvl(numeaven,0)) l,
 
(select numepoli,codeinte, nvl(avenmodi,0) numeaven,  min(dateeffe) dateeffe, min(dateeche) dateeche from orass_v6.hist_police where  nvl(avenmodi,0)=0
group by numepoli,codeinte, nvl(avenmodi,0)) m

where c.codecate = x.caterisq

and x.numepoli=k.numepoli(+)
and x.numeaven=k.numeaven (+)
and x.codeinte=k.codeinte (+)

and x.numepoli=l.numepoli(+)
and x.numeaven=l.numeaven (+)
and x.codeinte=l.codeinte (+)
--and x.codecate=l.codecate (+)
--and x.codeassu=l.codeassu (+)

and x.numepoli=m.numepoli(+)
and x.numeaven=m.numeaven (+)
and x.codeinte=m.codeinte (+)

and x.codeinte=i.codeinte
 and x.codegara=g.codegara  
 and c.codebran = b.codebran
 and   x.codeinte !=9999
--and  c.codebran = 81

--and  X.codeinte=3023 and x.numesini=300278 and to_char(x.datesurv,'yyyy')=2016
--and  x.codeinte=1021 and x.numesini=8214 and to_char(x.datesurv,'yyyy')=1990
--and  x.codeinte=3002 and x.numesini=13731 and to_char(x.datesurv,'yyyy')=1997
--and  x.codeinte=3044 and x.numesini=500232 and to_char(x.datesurv,'yyyy')=2017
 --and  x.codeinte=1001 and x.numesini=239 and to_char(x.datesurv,'yyyy')=1977

-- and x.codeassu in ( 10010028960,-30021122633,-30021104360 )
--and x.codeassu in (2850,410010016210)
 -- and c.codecate between   '400' and '412'
 --and  lower(x.nom_assu) like '%filivoire%'  
 --and x.codeinte|| x.NUMEPOLI in (3003||7010000001,3002||71263,3003||7010000110,)
group by 
month_id,
         x.codeinte,
        x.caterisq,
         c.codebran,
         i.raisocin,
         x.numesini,
         x.nom_assu,
         x.codeassu,
         c.libecate,
         x.caterisq,
         x.datesurv,
         x.refeinte,
         x.codegara,   
         x.datedecl,    
         g.libegara,  x.CODERISQ, 
         x.natusini,
          
         x.CODECATE,
         x.NUMEPOLI ,x.CODTYPSO ,x.LIBTYPSO , B.LIBEBRAN,x.NUMEAVEN,
         decode(k.dateeffe,NULL,decode(l.dateeffe, NULL,m.dateeffe,l.dateeffe),k.dateeffe) ,
         decode(k.dateeche,NULL,decode(l.dateeche, NULL,m.dateeche,l.dateeche),k.dateeche) 
        
         --decode(k.dateeffe,NULL,l.dateeffe, k.dateeffe) ,
        --decode(k.dateeche,NULL,l.dateeche,k.dateeche)  
--having sum(sap) > 0
order by 1,2,3;


delete from  ACTUARY.PN_RISQUE  
--where TO_CHAR (DATECOMP,'yyyymm')=month_id
;

insert into ACTUARY.PN_RISQUE
select  
  TYPEMOUV,
   GENRMOUV,
   CODTYPIN,
   i.NUMEQUIT,
   CODTYPQU,
   LIBTYPIN,
   i.CODEINTE,
   RAISOCIN,
   CODEASSU,
   NOM_ASSU,
   CODEBRAN,
   LIBEBRAN,
   CODECATE,
   LIBECATE,
   i.NUMEPOLI,
   CODERISQ,
  -- CODEGARA,
   NVL(NUMEAVEN,0) NUMEAVEN,
   NUMEAVEN_G,
   DATEEFFE,
   DATEECHE,
   DATECOMP,
   PRIMNETT PRIMNETT_POLICE,
   PRINETCO PRINETCO_POLICE,
   CHIFAFFA CHIFAFFA_POLICE,
   decode (TYPEMOUV,'ANNULATIONS',-primnette,primnette) primnett,
   case when nvl(PRIMNETT,0)=0 then decode (TYPEMOUV,'ANNULATIONS',-primnette,primnette) 
           else nvl(PRINETCO,0)/nvl(PRIMNETT,0)*decode (TYPEMOUV,'ANNULATIONS',-primnette,primnette) end primnetco --,
 
      
      
 from 
(select* 
 from  orass_v6.v_ch_affaire
 where  TO_CHAR (DATECOMP,'yyyymm')>='1990'
   --and codebran=4
 and  codeinte  not in (9999,9998,9995) and genrmouv<>'C')i,
 (
select
  CODEINTE, 
  NUMEPOLI,
  NUMEQUIT,
  NVL(NUMEAVEN,0) NUMEAVEN_G,
  CODERISQ,
  CODECATE CODECATE_G,
  CODEGARA,
  sum(primnett) primnette
  from orass_v6.prime_garantie
  where
  CODEINTE not in (9999,9998,9995) 
  group by
  CODEINTE, 
  NUMEPOLI,
  NUMEQUIT,
  NVL(NUMEAVEN,0) ,
  CODERISQ,
  CODECATE,
  CODEGARA
 --AND NUMEPOLI = 4000000271 and 
)j

where i.CODEINTE=j.CODEINTE(+) 
and   i.NUMEPOLI=j.NUMEPOLI(+) 
and   i.NUMEQUIT=j.NUMEQUIT(+) 
 ;

delete from  actuary.pn_all ;

insert into actuary.pn_all 
select
   CODEINTE ,TYPEMOUV,
   NUMEPOLI,NUMEQUIT,
   sum(primnett) primnett1,
   sum(primnetco) primnetco1
  from  actuary.PN_RISQUE

group by CODEINTE,TYPEMOUV,
   NUMEPOLI,NUMEQUIT
 ; 

delete from  actuary.PN_RISQUE1 ;

insert into actuary.PN_RISQUE1 

 select
   i.TYPEMOUV,
   GENRMOUV,
   CODTYPIN,
   i.NUMEQUIT,
   CODTYPQU,
   LIBTYPIN,
   i.CODEINTE,
   RAISOCIN,
   CODEASSU,
   NOM_ASSU,
   CODEBRAN,
   LIBEBRAN,
   CODECATE,
   LIBECATE,
   i.NUMEPOLI,
   CODERISQ,
   NUMEAVEN,
   NUMEAVEN_G,
   DATEEFFE,
   DATEECHE,
   DATECOMP,
   PRIMNETT_POLICE,
   PRINETCO_POLICE,
   CHIFAFFA_POLICE, 
  primnett,
  primnetco,
  case when nvl(primnett1,0)=0 then PRIMNETT_POLICE
      else nvl(primnett,0)/nvl(primnett1,0)*  PRIMNETT_POLICE end primnett2, 
   case when nvl(primnetco1,0)=0 then PRINETCO_POLICE
      else nvl (primnetco,0)/nvl(primnetco1,0)*  PRINETCO_POLICE end primnetco2, 
   case when nvl(primnett1,0)=0 then CHIFAFFA_POLICE
      else nvl(primnett,0)/nvl(primnett1,0)*  CHIFAFFA_POLICE end CHIFAFFA   
 from
  
(select *from actuary.PN_RISQUE)i,
(select
   *from actuary.pn_all)j

where i.CODEINTE=j.CODEINTE(+) 
and   i.NUMEPOLI=j.NUMEPOLI(+) 
and   i.NUMEQUIT=j.NUMEQUIT(+) 
and   i.TYPEMOUV=j.TYPEMOUV(+) 
;



delete from  actuary.tb_pn_risque_ref ;

insert into actuary.tb_pn_risque_ref 

select nvl(CODEBRAN,   0) CODEBRAN,
       nvl(BRANCHE1,   0) BRANCHE1,
       nvl(BRANCHE2,   0) BRANCHE2,
       nvl(NUM_POLICE, 0) NUM_POLICE, 
       nvl(NUMEPOLI,   0) NUMEPOLI,
       nvl(CODEINTE,   0) CODEINTE,
       nvl(CODERISQ, 0) CODERISQ, 
        IDPOLICE,MIN_YEAR,
       min(DATEEFFE)    DATEEFFE,
       min(datecomp)  datecomp,
                        DATEECHE,
       --sum(PRIMNETT2)    PRIMNETT_ALL,
       sum(PRIMNETCO2) PRIMNET,
       sum(CHIFAFFA)    CHIFAFFA,
       (trunc(DATEECHE)- trunc(min(DATEEFFE))+1) /365.25 EXPO
       --sum(COMMISSI)    COMMISSI,
      -- sum(REGLCOMM)     REGLCOMM,
     --  sum(COMMISS_CEDEE)         COMMISS_CEDEE,
     --  sum(COMISS_NETTE_DE_COASS) COMISS_NETTE_DE_COASS,
     --  sum(COMMISSION_APPORTEUR)  COMMISSION_APPORTEUR,
      -- sum(COMMGEST) COMMGEST
from

(
select
CODEBRAN,
BRANCHE1,
BRANCHE2,
CODEINTE,
NUMEPOLI,
nvl(CODEINTE, 0)||nvl(NUMEPOLI, 0) NUM_POLICE,
CODERISQ,
 nvl(CODEINTE, 0)||nvl(NUMEPOLI, 0)||nvl(CODERISQ,0)||nvl(to_char(DATEECHE),'0') IDPOLICE,
 trunc(DATEECHE) DATEECHE,
 trunc(DATEEFFE) DATEEFFE,
  trunc(DATECOMP) DATECOMP,
PRIMNETCO2,
--PRIME_CEDEE,
CHIFAFFA,
--,
--COMMISSI,
-- REGLCOMM,
--COMMISS_CEDEE,
--COMISS_NETTE_DE_COASS,
--COMMISSION_APPORTEUR,
--COMMGEST,
MIN_YEAR
from
(select *from actuary.pn_risque1 where PRIMNETCO2<>0  )i,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j,
(select EXTRACT(YEAR FROM min(DATEEFFE)) MIN_YEAR from actuary.pn_risque1 where '202111'>= to_char(datecomp,'YYYYMM') )k
where i.CODEBRAN=j.CBR(+) 

)
group by
nvl(CODEBRAN,   0)        ,
       nvl(BRANCHE1,   0) ,
       nvl(BRANCHE2,   0) ,
       nvl(NUM_POLICE, 0) , 
       nvl(NUMEPOLI,   0) ,
       nvl(CODEINTE,   0) ,
       nvl(CODERISQ, 0),
       IDPOLICE,
       DATEECHE,MIN_YEAR
      ; 


delete from  actuary.tb_pn_risque_ref1 ;
insert into  actuary.tb_pn_risque_ref1 
--create table actuary.tb_pn_risque_ref1  as
select
CODEBRAN,
NUM_POLICE,NUMEPOLI,CODEINTE,
min(DATEEFFE) DATEEFFE,
DATEECHE,
min(datecomp)  datecomp,
sum(PRIMNET) PRIMNET,
sum(EXPO) EXPO

from
(
select*from tb_pn_risque_ref
)  
 
 group by   NUM_POLICE,NUMEPOLI,CODEINTE,
DATEECHE ,CODEBRAN
 
 ; 
 
 -----------------------Table fusion-----------------

 --delete from  table_fusion  ;
 --insert into table_fusion  

--drop table table_fusion
--create table  table_fusion as (
-- select
 
--  to_char(tb_prim.DATEEFFE,'YYYY') ANNEE, 
-- tb_prim.CODEBRAN     ,
-- i.CODEINTE,
--  tb_iden.LIBTYPIN,
-- tb_iden.RAISOCIN,
-- tb_iden.CODEASSU,
-- tb_iden.NOM_ASSU,
-- tb_iden.CODECATE,
--
--k.GENRASSUB,
--k.CODEASSUB,
--k.NOMB,
--
--l.CODEINTEB,
--L.RAISOCINB,
--
-- i.NUMEPOLI,i.ID_FUSION,i.DATEECHE,
-- tb_prim.DATEEFFE,
-- tb_prim.PRIMNETT,
-- tb_prim.CHIFAFFA,
-- tb_prim.ACCEQUIT,
-- tb_prim.COMMISSI,
-- --tb_pri_ex.PRIMNET ,
-- tb_pri_ex.EXPO    ,
-- case when tb_pri_ex.EXPO is null then  Exposition(tb_prim.DATEEFFE,tb_prim.DATEECHE) else  tb_pri_ex.EXPO end EXPO_F,
--
--SINPAY,
--SINPAY_ANT,
--RECENC,
--RECENC_ANT,
--SAP,
--AREC,
--CHARGE_SIN,
--NBSIN
--
-- from
--
--(
--select distinct ID_FUSION,CODEINTE,NUMEPOLI,trunc(DATEECHE) DATEECHE
--from (select CODEINTE,NUMEPOLI,DATEECHE,
-- CODEINTE||'_'||NUMEPOLI||'_'||to_char(DATEECHE)  ID_FUSION
--from TABLE_PRIME
--UNION ALL
--  select CODEINTE,NUMEPOLI,DATEECHE, CODEINTE||'_'||NUMEPOLI||'_'||to_char(DATEECHE)  ID_FUSION from CHARGE_SINISTRE)
--
-- )i,
-- 
-- (
-- 
-- select
-- CODEINTE,CODEBRAN,
--  NUMEPOLI,ID_FUSION,trunc(DATEECHE) DATEECHE,
--    min(trunc(DATEEFFE)) DATEEFFE,
--    sum(PRIMNETTCO) PRIMNETT,
--    sum(CHIFAFFA) CHIFAFFA,
--    sum(ACCEQUIT) ACCEQUIT,
--    sum(COMMISSI) COMMISSI
--  
--  
-- from 
-- 
-- (
-- 
-- select 
-- LIBTYPIN  ,  
--CODEINTE   ,
--RAISOCIN   ,
--CODEASSU   ,
--NOM_ASSU   ,
--CODEBRAN   ,
--LIBEBRAN   ,
--CODECATE   ,
--LIBECATE   ,
--NUMEPOLI   ,
--NUMEAVEN   ,
--DATEEFFE   ,
--DATEECHE   ,
--DATECOMP   ,
--PRIMNETT   ,
--PRIMNETTCO ,
--CHIFAFFA   ,
--ACCEQUIT   ,
--COMMISSI   ,
--CODEINTE||'_'||NUMEPOLI||'_'||to_char(DATEECHE)  ID_FUSION
--from TABLE_PRIME)
--
--
--group by CODEINTE,CODEBRAN,
--  NUMEPOLI,ID_FUSION,trunc(DATEECHE)
--)tb_prim,
--
--
--(
--select
--CODEBRAN     ,
--NUM_POLICE   ,
--NUMEPOLI     ,
--CODEINTE     ,
--DATEEFFE     ,
--DATEECHE     ,
----DATECOMP     ,
--PRIMNET      ,
--EXPO         ,
--CODEINTE||'_'||NUMEPOLI||'_'||to_char(DATEECHE)  ID_FUSION
--from TB_PN_RISQUE_REF1
--)tb_pri_ex,
--
--(
--
----SELECT*FROM (
--select
-- CODEINTE||'_'||NUMEPOLI||'_'||to_char(DATEECHE)  ID_FUSION,
--CODEBRAN,LIBEBRAN,NUMEPOLI,--DATEEFFE,DATEECHE,
----CODEINTE,
--
--    max(LIBTYPIN) LIBTYPIN,
--    max(RAISOCIN) RAISOCIN,
--    max(CODEASSU) CODEASSU,
--    max(NOM_ASSU) NOM_ASSU,
--    max(CODECATE) CODECATE
--
--
--from TABLE_PRIME
--
--group by 
--CODEINTE||'_'||NUMEPOLI||'_'||to_char(DATEECHE) ,
--CODEBRAN,LIBEBRAN,NUMEPOLI--,DATEEFFE,DATEECHE,CODEINTE
----) where ID_FUSION='2508_4020000160_07/08/16'
--
--)tb_iden,
--
--(
--
--select
--
--CODEINTE,NUMEPOLI,ID_FUSION,--ANNEE ANNEE_SURV,
--
--sum(SINPAY)                             SINPAY,
--sum(SINPAY_ANT)                         SINPAY_ANT,
--sum(RECENC)                             RECENC,
--sum(RECENC_ANT)                         RECENC_ANT,
--sum(SAP)                                SAP,
--sum(AREC)                               AREC,
--sum(CHARGE_SIN)                         CHARGE_SIN,
--count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN
--
--from
--
--(
--select
--CODEINTE,ANNEE,NUMESINI,NUMEPOLI,ID_FUSION,
--sum(SINPAY) SINPAY,
--sum(SINPAY_ANT) SINPAY_ANT,
--sum(RECENC) RECENC,
--sum(RECENC_ANT) RECENC_ANT,
--sum(SAP) SAP,
--
--sum(AREC) AREC,
--sum(SINPAY)+ sum(SINPAY_ANT)+sum(SAP)- (sum(RECENC)+sum(RECENC_ANT)+sum(AREC)  )  CHARGE_SIN
--from
--
--(select
--CODEINTE||'_'||NUMEPOLI||'_'||to_char(DATEECHE)  ID_FUSION,
--ANNEE              ,      
--DEAL_DATE          ,
--CODEINTE           ,
--RAISOCIN           ,
--NUMESINI           ,
--REFEINTE           ,
--NUMEPOLI           ,
--NUMEAVEN           ,
--DATEEFFE           ,
--DATEECHE           ,
--CODEASSU           ,
--NOM                ,
--CODEBRAN           ,
--LIBEBRAN           ,
--CODECATE           ,
--CATERISQ           ,
--LIBECATE           ,
--CODEGARA           ,
--CODERISQ           ,
--NATUSINI           ,
--LIBEGARA           ,
--DATESURV           ,
--DATEDECL           ,
--SINPAY             ,
--SINPAY_ANT         ,
--RECENC             ,
--RECENC_ANT         ,
--SOLDE_PAIEMENTS    ,
--EVAL               ,
--SAP                ,
--AREC               ,
--COUTS_SINISTRES    ,
--CODTYPSO           ,
--LIBTYPSO           
--
--from
--CHARGE_SINISTRE)
--
--group by CODEINTE,ANNEE,NUMESINI,NUMEPOLI,ID_FUSION
--
--)  
--
--
--where CHARGE_SIN!=0
--group by CODEINTE,NUMEPOLI,ID_FUSION --,ANNEE
--
--
--)tb_sin,
--
--(select*from tb_assu)k,
--
--(select*from tb_inter) l
--
--where    i.ID_FUSION=tb_prim.ID_FUSION(+)
--      and i.CODEINTE=tb_prim.CODEINTE(+)
--      and i.NUMEPOLI=tb_prim.NUMEPOLI(+)
--      
--      and i.ID_FUSION=tb_sin.ID_FUSION(+)
--      and i.CODEINTE=tb_sin.CODEINTE(+)
--      and i.NUMEPOLI=tb_sin.NUMEPOLI(+)
--      
--      and i.ID_FUSION=tb_pri_ex.ID_FUSION(+)
--      and i.CODEINTE=tb_pri_ex.CODEINTE(+)
--      and i.NUMEPOLI=tb_pri_ex.NUMEPOLI(+)
--      
--     
--      and i.ID_FUSION=tb_iden.ID_FUSION(+)
--     -- and i.CODEINTE=tb_iden.CODEINTE(+)
--      and i.NUMEPOLI=tb_iden.NUMEPOLI(+)
--     -- and tb_prim.DATEEFFE=tb_iden.DATEEFFE(+)
--     -- and i.DATEECHE=tb_iden.DATEECHE(+)
--      
--      and tb_iden.CODEASSU=k.CODEASSU(+)
--      and tb_prim.CODEINTE=l.CODEINTE(+)
      
-- 
--)
-- ;
-----Calculer la prime et  acquise, emise, d'effet, l'exposition la charge sinistre par branche, categorie , intermediaire et police

     

     for an IN (itt+1)..fin   loop

     delete from actuary.tb_pn_risque_acq1 where annee_acquisition=an;
     insert into actuary.tb_pn_risque_acq1
  

   (select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEBRAN,
   BRANCHE1,
   BRANCHE2,
   CODEINTE,
   NUMEPOLI,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   CODEBRAN,
   BRANCHE1,
   BRANCHE2,
   NUM_POLICE,
   NUMEPOLI,
   CODEINTE,
   IDPOLICE,
   DATECOMP,
   DATEEFFE,
   DATEECHE,
   PRIMNET,
   CODERISQ,
   --PRIME_CEDEE,
   CHIFAFFA,
   --COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an)) Acquisition,
   Exposition(DATEEFFE,DATEECHE) Expo,
   case when AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end kount,
   (case when AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end) *1 NbrePolice,
   an Annee_acquisition

from  actuary.tb_pn_risque_ref)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
   CODEBRAN,
   BRANCHE1,
   BRANCHE2,
   CODEINTE,
   NUMEPOLI

);
END LOOP;
  

  for an IN (itt+1)..fin   loop

     delete from actuary.tb_pn_risque_acq2 where annee_acquisition=an;
     insert into actuary.tb_pn_risque_acq2
  

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEBRAN,
   CODEINTE,
   NUMEPOLI,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_acquise) Prime_acquise,
   --sum(COMMISSI_acquise) commission_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise--pn_risque1
from
(select
   datecomp,
   CODERISQ,
   CODEBRAN,
   CODEINTE,
   NUMEPOLI,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when Exposition(DATEEFFE,DATEECHE)=0 then 0 else CHIFAFFA *AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Prime_acquise,
   case when Exposition(DATEEFFE,DATEECHE)=0 then 0 else PRIMNETCO2 *AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Pnette_acquise,
   --  case when Exposition(DATEEFFE,DATEECHE)=0 then 0 else COMMISSI *AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end COMMISSI_acquise,
    an Annee_acquisition

from  actuary.pn_risque1)
group by to_char(datecomp,'YYYY'), Annee_acquisition, CODEBRAN,CODEINTE,
   NUMEPOLI

);
END LOOP;



 commit;
 
  delete from actuary.dtm_actuary1;
  insert into actuary.dtm_actuary1
--drop table  actuary.dtm_actuary1
--create table actuary.dtm_actuary1  as (
 select
 
  tb.ANNEE,
  tb.CODEBRAN,
  tb.CODEINTE,
  tb.NUMEPOLI,
  PRIME_EMISE,PRIMNETT_EMISSION,PARC_RISQUE,NB_AVENANT,ACCEQUIT_EMISE,COMISSION_EMISE,
  PRIME_EFFET,PRIMNETT_EFFET,ACCEQUIT_EFFET,COMISSION_EFFET,
   EXPOSITION,PRIME_ACQUISE,PRIMNETT_ACQUISE,
   SINPAY,SINPAY_ANT,RECENC,RECENC_ANT,SAP,AREC,CHARGE_SIN_SURVENANCE,NBSIN_SURVENANCE,
   CHARGE_SIN_EFFET, NBSIN_EFFET,
   CHARGE_DTSOINS,CHARGE_COMPTABLE,CHARGE_DTSOINS_GEST,CHARGE_COMPTABLE_GEST

 from
 
  --fusion
 
   (select distinct ANNEE,CODEBRAN,CODEINTE,NUMEPOLI from 
   (select to_char(datecomp,'yyyy') ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI from actuary.PN_RISQUE1
 union all select to_char(dateeffe,'yyyy') ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI from actuary.TABLE_PRIME
  union all select to_char(datecomp,'yyyy') ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI from actuary.TABLE_PRIME
 union all select to_char(annee_acquisition) ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI from actuary.tb_pn_risque_acq1
  union all select to_char(annee_acquisition) ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI from actuary.tb_pn_risque_acq2
  union all select to_char(ANNEE_COMPTABLE) ANNEE, 81 CODEBRAN,to_char(nvl(CODEINTE,0)) CODEINTE,to_char(nvl(NUMEPOLI,0))  NUMEPOLI from actuary.sinistre_health
  union all select to_char(ANNEE_SURV) ANNEE, 81 CODEBRAN,to_char(nvl(CODEINTE,0)) CODEINTE,to_char(nvl(NUMEPOLI,0))  NUMEPOLI from actuary.sinistre_health
   union all select to_char(ANNEE_COMPTABLE) ANNEE, 81 CODEBRAN,to_char(nvl(GESTIONNAIRE,'4')) CODEINTE,to_char(nvl(NUMEPOLI,0))  NUMEPOLI from actuary.sinistre_health
  union all select to_char(ANNEE_SURV) ANNEE, 81 CODEBRAN,to_char(nvl(GESTIONNAIRE,'4')) CODEINTE,to_char(nvl(NUMEPOLI,0))  NUMEPOLI from actuary.sinistre_health
   union all select to_char(dateeffe,'yyyy') ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI from  actuary.CHARGE_SINISTRE
   union all select  ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI from  actuary.CHARGE_SINISTRE
  ))tb,
     
 --emission
    (select
    
    to_char(DATECOMP,'YYYY')  ANNEE,
    CODEBRAN,
   -- CODECATE,
    to_char(CODEINTE) CODEINTE,
    to_char(NUMEPOLI) NUMEPOLI,
    
    sum(CHIFAFFA) PRIME_EMISE,
    sum(PRIMNETCO2) PRIMNETT_EMISSION,
    count(distinct CODEINTE||'_'||NUMEPOLI||'_'||CODERISQ) PARC_RISQUE,
    --count(distinct CODEINTE||'_'||NUMEPOLI) PARC_POLICE,
    count(*) NB_AVENANT
    
    from actuary.PN_RISQUE1 -- where CODEINTE='3028' and numepoli='84506' and  to_char(DATECOMP,'YYYY')='2012'
    
    group by 
    
    to_char(DATECOMP,'YYYY') , 
    CODEBRAN,
    --CODECATE,
    to_char(CODEINTE) ,
    to_char(NUMEPOLI)
    ) i,
    
    
    ---vu effet
    (select
    
    to_char(DATEEFFE,'YYYY')  ANNEE,
    CODEBRAN,
   to_char(CODEINTE) CODEINTE,
    to_char(NUMEPOLI) NUMEPOLI,
    sum(CHIFAFFA) PRIME_EFFET,
    sum(PRIMNETTCO) PRIMNETT_EFFET,
    sum(ACCEQUIT) ACCEQUIT_EFFET,
    sum(COMMISSI) COMISSION_EFFET
    from actuary.TABLE_PRIME
    group by 
    to_char(DATEEFFE,'YYYY') , 
    CODEBRAN,
    to_char(CODEINTE) ,
    to_char(NUMEPOLI) ) j,
    
    --comm emise
    (select
    
    to_char(DATECOMP,'YYYY')  ANNEE,
    CODEBRAN,
    to_char(CODEINTE) CODEINTE,
    to_char(NUMEPOLI) NUMEPOLI,
    sum(ACCEQUIT) ACCEQUIT_EMISE,
    sum(COMMISSI) COMISSION_EMISE
    from actuary.TABLE_PRIME
    group by 
    to_char(DATECOMP,'YYYY') , 
    CODEBRAN,
    to_char(CODEINTE) ,
    to_char(NUMEPOLI) ) k,
    
    ---exposition
    (
     select
     CODEBRAN,
     to_char(ANNEE_ACQUISITION) ANNEE,
     to_char(CODEINTE) CODEINTE,
    to_char(NUMEPOLI) NUMEPOLI,
     sum(ACQUISITION) EXPOSITION
     
     from actuary.tb_pn_risque_acq1 group by 
    CODEBRAN,
     to_char(ANNEE_ACQUISITION),
     to_char(CODEINTE) ,
    to_char(NUMEPOLI) )l,
    
---prime acquise
    
    (select 
     CODEBRAN,
     to_char(ANNEE_ACQUISITION) ANNEE,
     to_char(CODEINTE) CODEINTE,
    to_char(NUMEPOLI) NUMEPOLI,
     sum(PRIME_ACQUISE) PRIME_ACQUISE,
     sum(PNETTE_ACQUISE) PRIMNETT_ACQUISE
     
    from actuary.tb_pn_risque_acq2 group by 
    CODEBRAN,
    to_char(ANNEE_ACQUISITION),
     to_char(CODEINTE) ,
    to_char(NUMEPOLI) 
    )m,
    
    
  ---  charge vu par annee survenance
    (
    select
    ANNEE,
    to_char(CODEINTE) CODEINTE ,CODEBRAN,
    to_char(NUMEPOLI) NUMEPOLI,--ANNEE ANNEE_SURV,
    
    sum(SINPAY)                             SINPAY,
    sum(SINPAY_ANT)                         SINPAY_ANT,
    sum(RECENC)                             RECENC,
    sum(RECENC_ANT)                         RECENC_ANT,
    sum(SAP)                                SAP,
    sum(AREC)                               AREC,
    sum(CHARGE_SIN)                         CHARGE_SIN_SURVENANCE,
    count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN_SURVENANCE
    
    from
    
    (
    select
    to_char(ANNEE) ANNEE,CODEINTE,NUMEPOLI,NUMESINI,CODEBRAN,
    sum(SINPAY) SINPAY,
    sum(SINPAY_ANT) SINPAY_ANT,
    sum(RECENC) RECENC,
    sum(RECENC_ANT) RECENC_ANT,
    sum(SAP) SAP,
    
    sum(AREC) AREC,
    sum(SINPAY)+ sum(SINPAY_ANT)+sum(SAP)- (sum(RECENC)+sum(RECENC_ANT)+sum(AREC)  )  CHARGE_SIN
    --count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN
    from
    
    (select*
    from
    actuary.CHARGE_SINISTRE)
    
    group by CODEINTE,to_char(ANNEE),NUMESINI,NUMEPOLI,CODEBRAN
    
    )  
    where CHARGE_SIN!=0
    group by to_char(CODEINTE)  ,
    to_char(NUMEPOLI) ,ANNEE,CODEBRAN

    )ci,
    
   
   --charge vu par couverture des polices

    ( 
    select
    ANNEE,CODEBRAN,
    to_char(CODEINTE) CODEINTE ,
    to_char(NUMEPOLI) NUMEPOLI,--ANNEE ANNEE_SURV,
    
    --sum(SINPAY)                             SINPAY,
   -- sum(SINPAY_ANT)                         SINPAY_ANT,
   -- sum(RECENC)                             RECENC,
   -- sum(RECENC_ANT)                         RECENC_ANT,
   -- sum(SAP)                                SAP,
   -- sum(AREC)                               AREC,
    sum(CHARGE_SIN)                         CHARGE_SIN_EFFET,
    count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN_EFFET
    
    from
    
    (
    select
    to_char(dateeffe, 'YYYY') ANNEE,CODEINTE,NUMEPOLI,NUMESINI,CODEBRAN,
    sum(SINPAY) SINPAY,
    sum(SINPAY_ANT) SINPAY_ANT,
    sum(RECENC) RECENC,
    sum(RECENC_ANT) RECENC_ANT,
    sum(SAP) SAP,
    
    sum(AREC) AREC,
    sum(SINPAY)+ sum(SINPAY_ANT)+sum(SAP)- (sum(RECENC)+sum(RECENC_ANT)+sum(AREC)  )  CHARGE_SIN
    --count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN
    from
    
    (select*
    from
    actuary.CHARGE_SINISTRE)
    
    group by CODEINTE,to_char(dateeffe, 'YYYY'),NUMESINI,NUMEPOLI,CODEBRAN
    
    )  
    where CHARGE_SIN!=0
    group by to_char(CODEINTE)  ,
    to_char(NUMEPOLI) ,ANNEE,CODEBRAN
    
    )cj,

   ---charge sante par date soins
   (
   select
   ANNEE_SURV ANNEE, 81 CODEBRAN,
   nvl(CODEINTE,0) CODEINTE,
    nvl(NUMEPOLI,0) NUMEPOLI,
   sum(CHARGE) CHARGE_DTSOINS
   from actuary.sinistre_health group by ANNEE_SURV,
   nvl(CODEINTE,0),81,
    nvl(NUMEPOLI,0)
   ) ck,


  --charge sante par date comptable
   (
   select
   ANNEE_COMPTABLE ANNEE,
   81 CODEBRAN,
  nvl(CODEINTE,0) CODEINTE,
    nvl(NUMEPOLI,0) NUMEPOLI,
   sum(CHARGE) CHARGE_COMPTABLE
   from actuary.sinistre_health group by ANNEE_COMPTABLE, 81,
   nvl(CODEINTE,0) ,
    nvl(NUMEPOLI,0) 
   ) cl,
   
    (
   select
   ANNEE_COMPTABLE ANNEE, 81 CODEBRAN,
   nvl(GESTIONNAIRE,'4') CODEINTE,
    nvl(NUMEPOLI,0) NUMEPOLI,
   sum(CHARGE) CHARGE_COMPTABLE_GEST
   from actuary.sinistre_health group by ANNEE_COMPTABLE,
   nvl(GESTIONNAIRE,'4'),81,
    nvl(NUMEPOLI,0)
   ) cm,
   
   (
   select
   ANNEE_SURV ANNEE, 81 CODEBRAN,
   nvl(GESTIONNAIRE,'4') CODEINTE,
    nvl(NUMEPOLI,0) NUMEPOLI,
   sum(CHARGE) CHARGE_DTSOINS_GEST
   from actuary.sinistre_health group by ANNEE_SURV,
   nvl(GESTIONNAIRE,'4'),81,
    nvl(NUMEPOLI,0)
   ) cn

where

         tb.ANNEE=i.ANNEE(+) and tb.CODEBRAN=i.CODEBRAN(+) and tb.CODEINTE=i.CODEINTE(+) and tb.NUMEPOLI=i.NUMEPOLI(+)
     
     and tb.ANNEE=j.ANNEE(+) and tb.CODEBRAN=j.CODEBRAN(+) and tb.CODEINTE=j.CODEINTE(+) and tb.NUMEPOLI=j.NUMEPOLI(+)
     
     and tb.ANNEE=k.ANNEE(+) and tb.CODEBRAN=k.CODEBRAN(+) and tb.CODEINTE=k.CODEINTE(+) and tb.NUMEPOLI=k.NUMEPOLI(+)
     
     and tb.ANNEE=l.ANNEE(+) and tb.CODEBRAN=l.CODEBRAN(+) and tb.CODEINTE=l.CODEINTE(+) and tb.NUMEPOLI=l.NUMEPOLI(+)
     
     and tb.ANNEE=m.ANNEE(+)and tb.CODEBRAN=m.CODEBRAN(+) and tb.CODEINTE=m.CODEINTE(+) and tb.NUMEPOLI=m.NUMEPOLI(+)
     
     and tb.ANNEE=ci.ANNEE(+)and tb.CODEBRAN=ci.CODEBRAN(+) and tb.CODEINTE=ci.CODEINTE(+) and tb.NUMEPOLI=ci.NUMEPOLI(+)
     and tb.ANNEE=cj.ANNEE(+)and tb.CODEBRAN=cj.CODEBRAN(+) and tb.CODEINTE=cj.CODEINTE(+) and tb.NUMEPOLI=cj.NUMEPOLI(+)
     and tb.ANNEE=ck.ANNEE(+)and tb.CODEBRAN=ck.CODEBRAN(+) and tb.CODEINTE=ck.CODEINTE(+) and tb.NUMEPOLI=ck.NUMEPOLI(+)
     and tb.ANNEE=cl.ANNEE(+)and tb.CODEBRAN=cl.CODEBRAN(+) and tb.CODEINTE=cl.CODEINTE(+) and tb.NUMEPOLI=cl.NUMEPOLI(+)
     and tb.ANNEE=cm.ANNEE(+)and tb.CODEBRAN=cm.CODEBRAN(+) and tb.CODEINTE=cm.CODEINTE(+) and tb.NUMEPOLI=cm.NUMEPOLI(+)
     and tb.ANNEE=cn.ANNEE(+)and tb.CODEBRAN=cn.CODEBRAN(+) and tb.CODEINTE=cn.CODEINTE(+) and tb.NUMEPOLI=cn.NUMEPOLI(+)

----)
;


  delete from actuary.dtm_actuary_p  where view_date=deal_date;
    
  insert into actuary.dtm_actuary_p

--drop table actuary.dtm_actuary_p
-- create table actuary.dtm_actuary_p as (
 select
 
   i.ANNEE, 
  deal_date view_date,
 --'20220228' view_date,
  i.CODEBRAN,k.LIBEBRAN,k.LIBEBRAN1,k.LIBEBRAN2,
 case when j.codecate is null then m.codecate ELSE j.codecate  end codecate,
  case when j.RAISOCIN is null then m.RAISOCIN ELSE j.RAISOCIN  end RAISOCIN,
  case when j.RAISOCINB is null then m.RAISOCINB ELSE j.RAISOCINB  end RAISOCINB,
  j.LIBTYPIN LIBTYPIN,
  i.CODEINTE,
  
  case when j.CODEASSU is null then m.CODEASSU ELSE j.CODEASSU  end CODEASSU,
  case when j.CODEASSUB is null then m.CODEASSUB ELSE j.CODEASSUB  end CODEASSUB,
  case when j.NOM_ASSU is null then m.NOM_ASSU ELSE j.NOM_ASSU  end NOM_ASSU,
  case when j.NOMB is null then m.NOMB ELSE j.NOMB  end NOM_ASSUB,
  
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
   nvl(CHARGE_SIN_EFFET         ,0) CHARGE_SIN_EFFET     ,
   nvl(NBSIN_EFFET              ,0) NBSIN_EFFET          ,
   nvl(CHARGE_DTSOINS           ,0) CHARGE_DTSOINS       ,
   nvl(CHARGE_COMPTABLE         ,0) CHARGE_COMPTABLE     ,
    nvl(CHARGE_DTSOINS_GEST           ,0) CHARGE_DTSOINS_GEST       ,
   nvl(CHARGE_COMPTABLE_GEST         ,0) CHARGE_COMPTABLE_GEST     
  
  
  from
 (select* from actuary.dtm_actuary1)i,
 (
 select
 --ANNEE,
 i.CODEINTE,CODEINTEB, NUMEPOLI, LIBTYPIN,i.RAISOCIN,RAISOCINB, i.CODEASSU,CODEASSUB,i.NOM_ASSU,NOMB, CODECATE
 
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
 i.CODEINTE,CODEINTEB, NUMEPOLI,i.RAISOCIN,RAISOCINB, i.CODEASSU,CODEASSUB,i.NOM_ASSU,NOMB, CODECATE
 
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

delete from actuary.dtm_actuary ; --where view_date=deal_date;
    
  insert into actuary.dtm_actuary
  
 -- drop table actuary.dtm_actuary
  --create table actuary.dtm_actuary as (
  
  select
        i.ANNEE                                                                       ,
        VIEW_DATE                                                                     ,
        CODEBRAN                                                                      ,
        LIBEBRAN                                                                      ,
        LIBEBRAN1                                                                     ,
        LIBEBRAN2                                                                     ,
        CODECATE                                                                      ,
        case when RAISOCIN is null then  RAISOCINg else RAISOCIN end RAISOCIN         ,
        case when RAISOCINB is null then  RAISOCINBg else RAISOCINB end RAISOCINB     ,
        case when LIBTYPIN is null then  LIBTYPINg else LIBTYPIN end LIBTYPIN         ,    
        i.CODEINTE                                                                    ,
        CODEASSU                                                                      ,
        CODEASSUB                                                                     ,
        NOM_ASSU                                                                      ,
        NOM_ASSUB                                                                     ,
        NUMEPOLI                                                                      ,
        PRIME_EMISE                                                                   ,
        PRIMNETT_EMISE                                                                ,
        PARC_RISQUE                                                                   ,
        NB_AVENANT                                                                    ,
        ACCEQUIT_EMISE                                                                ,
        COMISSION_EMISE                                                               ,
        PRIME_EFFET                                                                   ,
        PRIMNETT_EFFET                                                                ,
        ACCEQUIT_EFFET                                                                ,
        COMISSION_EFFET                                                               ,
        EXPOSITION                                                                    ,
        PRIME_ACQUISE                                                                 ,
        PRIMNETT_ACQUISE                                                              ,
        SINPAY                                                                        ,
        SINPAY_ANT                                                                    ,
        RECENC                                                                        ,
        RECENC_ANT                                                                    ,
        SAP                                                                           ,
        AREC                                                                          ,
        CHARGE_SIN_SURVENANCE                                                         ,
        NBSIN_SURVENANCE                                                              ,
        CHARGE_SIN_EFFET                                                              ,
        NBSIN_EFFET                                                                   ,
        CHARGE_DTSOINS                                                                ,
        CHARGE_COMPTABLE                                                              ,
        CHARGE_DTSOINS_GEST                                                           ,
        CHARGE_COMPTABLE_GEST

  from 
  (select*from actuary.dtm_actuary_p)i,
  (select 
   ANNEE,CODEINTE,
    max(RAISOCIN) RAISOCINg,
   	max(RAISOCINB) RAISOCINBg,
   	max(LIBTYPIN) LIBTYPINg

  from dtm_actuary_p group by ANNEE,CODEINTE)j
where 
    i.CODEINTE=j.CODEINTE(+)
and i.ANNEE=j.ANNEE(+)
  
 --)
;
 

delete from actuary.DTM_CONTROL1;

insert into actuary.DTM_CONTROL1

--drop table actuary.DTM_CONTROL1
--create table  actuary.DTM_CONTROL1 as (
select
   
   i.ANNEE,
   round(I.PRIME_EMISE-em.PRIME_EMISE,4) ECART_PRIME_EMISE,
   round(I.PRIME_EFFET-ef.PRIME_EFFET,4) ECART_PRIME_EFFET,
   round(I.PRIMNETT_EFFET-ef.PRIMNETT_EFFET,4) ECART_PRIMNETT_EFFET,
   round(I.ACCEQUIT_EFFET-ef.ACCEQUIT_EFFET,4) ECART_ACCEQUIT_EFFET,
   round(I.COMISSION_EFFET-ef.COMISSION_EFFET,4) ECART_COMISSION_EFFET,
   round(I.ACCEQUIT_EMISE-cm.ACCEQUIT_EMISE,4) ECART_ACCEQUIT_EMISE,
   round(I.COMISSION_EMISE-cm.COMISSION_EMISE,4) ECART_COMISSION_EMISE,
   round(I.EXPOSITION-ex.EXPOSITION,4) ECART_EXPOSITION,
   round(I.PRIME_ACQUISE-pa.PRIME_ACQUISE,4) ECART_PRIME_ACQUISE,
   round(I.PRIMNETT_ACQUISE-pa.PNETTE_ACQUISE,4) ECART_PNETTE_ACQUISE,
   round(I.CHARGE_SIN_SURVENANCE-si.CHARGE_SIN,4) ECART_CHARGE_SIN,
   round(I.CHARGE_DTSOINS-dts.CHARGE_DTSOINS,4) ECART_CHARGE_DTSOINS,
   round(I.CHARGE_COMPTABLE-dtc.CHARGE_COMPTABLE,4) ECART_CHARGE_COMPTABLE
   
   
   
from 
(
select
ANNEE, sum(PRIME_EMISE) PRIME_EMISE, sum(PRIME_EFFET) PRIME_EFFET, sum(PRIMNETT_EFFET) PRIMNETT_EFFET, 
sum(ACCEQUIT_EFFET) ACCEQUIT_EFFET, sum(COMISSION_EFFET) COMISSION_EFFET, sum(ACCEQUIT_EMISE) ACCEQUIT_EMISE, 
 sum(COMISSION_EMISE) COMISSION_EMISE, sum(EXPOSITION) EXPOSITION,
 sum(PRIME_ACQUISE) PRIME_ACQUISE, sum(PRIMNETT_ACQUISE) PRIMNETT_ACQUISE, sum(CHARGE_SIN_SURVENANCE) CHARGE_SIN_SURVENANCE, 
 sum(CHARGE_DTSOINS) CHARGE_DTSOINS, sum(CHARGE_COMPTABLE) CHARGE_COMPTABLE
from actuary.dtm_actuary group by ANNEE )i,

(select  to_char(DATECOMP,'YYYY')  ANNEE, sum(CHIFAFFA) PRIME_EMISE  from actuary.PN_RISQUE1
        group by to_char(DATECOMP,'YYYY')  order by to_char(DATECOMP,'YYYY')) em,

(select  to_char(DATEEFFE,'YYYY')  ANNEE,sum(CHIFAFFA) PRIME_EFFET,sum(PRIMNETTCO) PRIMNETT_EFFET,sum(ACCEQUIT) ACCEQUIT_EFFET,sum(COMMISSI) COMISSION_EFFET
        from actuary.TABLE_PRIME group by to_char(DATEEFFE,'YYYY')  order by to_char(DATEEFFE,'YYYY')) ef,

(select  to_char(DATECOMP,'YYYY')  ANNEE,sum(ACCEQUIT) ACCEQUIT_EMISE, sum(COMMISSI) COMISSION_EMISE
        from actuary.TABLE_PRIME group by  to_char(DATECOMP,'YYYY') order by to_char(DATECOMP,'YYYY')) cm,

(select ANNEE_ACQUISITION ANNEE, sum(ACQUISITION) EXPOSITION from actuary.tb_pn_risque_acq1 group by ANNEE_ACQUISITION order by ANNEE_ACQUISITION)ex,
(select ANNEE_ACQUISITION ANNEE, sum(PRIME_ACQUISE) PRIME_ACQUISE,sum(PNETTE_ACQUISE) PNETTE_ACQUISE from actuary.tb_pn_risque_acq2
 group by ANNEE_ACQUISITION order by ANNEE_ACQUISITION)pa,
 
 (select  ANNEE, sum(SINPAY)+ sum(SINPAY_ANT)+sum(SAP)- (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_SIN from actuary.CHARGE_SINISTRE group by ANNEE  order by ANNEE)si,
 
 (select ANNEE_SURV ANNEE,sum(CHARGE) CHARGE_DTSOINS from actuary.sinistre_health group by ANNEE_SURV order by ANNEE_SURV) dts,
 (select ANNEE_COMPTABLE ANNEE,sum(CHARGE) charge_comptable from actuary.sinistre_health group by ANNEE_COMPTABLE order by ANNEE_COMPTABLE) dtc
 
 
where
      i.ANNEE=em.ANNEE(+)
 and  i.ANNEE=ef.ANNEE(+)
 and  i.ANNEE=cm.ANNEE(+)
 and  i.ANNEE=ex.ANNEE(+)
 and  i.ANNEE=pa.ANNEE(+)
 and  i.ANNEE=si.ANNEE(+)
 and  i.ANNEE=dts.ANNEE(+)
 and  i.ANNEE=dtc.ANNEE(+)
 
 --)
 ;
 
 delete from actuary.DTM_CONTROL2;

insert into actuary.DTM_CONTROL2

--drop table actuary.DTM_CONTROL2
--create table  actuary.DTM_CONTROL2 as (
select
   
   i.ANNEE,i.CODEBRAN,i.CODEINTE, i.NUMEPOLI,
   round(nvl(i.PRIME_EMISE,0)-  nvl(em.PRIME_EMISE,0),4) ECART_PRIME_EMISE,
   round(nvl(i.PRIME_EFFET,0)-nvl(ef.PRIME_EFFET,0),4) ECART_PRIME_EFFET,
   round(nvl(i.PRIMNETT_EFFET,0)-nvl(ef.PRIMNETT_EFFET,0),4) ECART_PRIMNETT_EFFET,
   round(nvl(i.ACCEQUIT_EFFET,0)-nvl(ef.ACCEQUIT_EFFET,0),4) ECART_ACCEQUIT_EFFET,
   round(nvl(i.COMISSION_EFFET,0)-nvl(ef.COMISSION_EFFET,0),4) ECART_COMISSION_EFFET,
   round(nvl(i.ACCEQUIT_EMISE,0)-nvl(cm.ACCEQUIT_EMISE,0),4) ECART_ACCEQUIT_EMISE,
   round(nvl(i.COMISSION_EMISE,0)-nvl(cm.COMISSION_EMISE,0),4) ECART_COMISSION_EMISE,
   round(nvl(i.EXPOSITION,0)-nvl(ex.EXPOSITION,0),4) ECART_EXPOSITION,
   round(nvl(i.PRIME_ACQUISE,0)-nvl(pa.PRIME_ACQUISE,0),4) ECART_PRIME_ACQUISE,
   round(nvl(i.PRIMNETT_ACQUISE,0)-nvl(pa.PNETTE_ACQUISE,0),4) ECART_PNETTE_ACQUISE,
   round(nvl(i.CHARGE_SIN_SURVENANCE,0)-nvl(si.CHARGE_SIN,0),4) ECART_CHARGE_SIN,
   round(nvl(i.CHARGE_DTSOINS,0)-nvl(dts.CHARGE_DTSOINS,0),4) ECART_CHARGE_DTSOINS,
   round(nvl(i.CHARGE_COMPTABLE,0)-nvl(dtc.CHARGE_COMPTABLE,0),4) ECART_CHARGE_COMPTABLE
   
   
   
from 
(
select CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI,
ANNEE, sum(PRIME_EMISE) PRIME_EMISE, sum(PRIME_EFFET) PRIME_EFFET, sum(PRIMNETT_EFFET) PRIMNETT_EFFET, 
sum(ACCEQUIT_EFFET) ACCEQUIT_EFFET, sum(COMISSION_EFFET) COMISSION_EFFET, sum(ACCEQUIT_EMISE) ACCEQUIT_EMISE, 
 sum(COMISSION_EMISE) COMISSION_EMISE, sum(EXPOSITION) EXPOSITION,
 sum(PRIME_ACQUISE) PRIME_ACQUISE, sum(PRIMNETT_ACQUISE) PRIMNETT_ACQUISE, sum(CHARGE_SIN_SURVENANCE) CHARGE_SIN_SURVENANCE, 
 sum(CHARGE_DTSOINS) CHARGE_DTSOINS, sum(CHARGE_COMPTABLE) CHARGE_COMPTABLE
from actuary.dtm_actuary group by ANNEE ,CODEBRAN,to_char(CODEINTE) , to_char(NUMEPOLI)  )i,

(select  to_char(DATECOMP,'YYYY')  ANNEE, CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI,sum(CHIFAFFA) PRIME_EMISE  from actuary.PN_RISQUE1
        group by to_char(DATECOMP,'YYYY'),CODEBRAN,to_char(CODEINTE) , to_char(NUMEPOLI) order by to_char(DATECOMP,'YYYY')) em,

(select  to_char(DATEEFFE,'YYYY')  ANNEE, CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI,sum(CHIFAFFA) PRIME_EFFET,sum(PRIMNETTCO) PRIMNETT_EFFET,sum(ACCEQUIT) ACCEQUIT_EFFET,sum(COMMISSI) COMISSION_EFFET
        from actuary.TABLE_PRIME group by to_char(DATEEFFE,'YYYY'),CODEBRAN,to_char(CODEINTE) , to_char(NUMEPOLI)  order by to_char(DATEEFFE,'YYYY')) ef,

(select  to_char(DATECOMP,'YYYY')  ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI,sum(ACCEQUIT) ACCEQUIT_EMISE, sum(COMMISSI) COMISSION_EMISE
        from actuary.TABLE_PRIME group by  to_char(DATECOMP,'YYYY'),CODEBRAN,to_char(CODEINTE) , to_char(NUMEPOLI)order by to_char(DATECOMP,'YYYY')) cm,

(select ANNEE_ACQUISITION ANNEE, CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI,sum(ACQUISITION) EXPOSITION from actuary.tb_pn_risque_acq1 group by ANNEE_ACQUISITION,CODEBRAN,to_char(CODEINTE) , to_char(NUMEPOLI) order by ANNEE_ACQUISITION)ex,
(select ANNEE_ACQUISITION ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI, sum(PRIME_ACQUISE) PRIME_ACQUISE,sum(PNETTE_ACQUISE) PNETTE_ACQUISE from actuary.tb_pn_risque_acq2
 group by ANNEE_ACQUISITION ,CODEBRAN,to_char(CODEINTE) , to_char(NUMEPOLI)  order by ANNEE_ACQUISITION)pa,
 
 (select  ANNEE,CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI, sum(SINPAY)+ sum(SINPAY_ANT)+sum(SAP)- (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_SIN from actuary.CHARGE_SINISTRE group by ANNEE,CODEBRAN,to_char(CODEINTE) , to_char(NUMEPOLI) order by ANNEE)si,
 
 (select ANNEE_SURV ANNEE, 81 CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI, sum(CHARGE) CHARGE_DTSOINS from actuary.sinistre_health group by ANNEE_SURV,81,to_char(CODEINTE) , to_char(NUMEPOLI) order by ANNEE_SURV) dts,
 (select ANNEE_COMPTABLE ANNEE,81 CODEBRAN,to_char(CODEINTE) CODEINTE, to_char(NUMEPOLI) NUMEPOLI,sum(CHARGE) charge_comptable from actuary.sinistre_health group by ANNEE_COMPTABLE,81 ,to_char(CODEINTE) , to_char(NUMEPOLI) order by ANNEE_COMPTABLE) dtc
 
 
where
      i.ANNEE=em.ANNEE(+) and i.CODEBRAN=em.CODEBRAN(+) and i.CODEINTE=em.CODEINTE(+)  and i.NUMEPOLI=em.NUMEPOLI(+)
 and  i.ANNEE=ef.ANNEE(+) and i.CODEBRAN=ef.CODEBRAN(+) and i.CODEINTE=ef.CODEINTE(+)  and i.NUMEPOLI=ef.NUMEPOLI(+)
 and  i.ANNEE=cm.ANNEE(+) and i.CODEBRAN=cm.CODEBRAN(+) and i.CODEINTE=cm.CODEINTE(+)  and i.NUMEPOLI=cm.NUMEPOLI(+)
 and  i.ANNEE=ex.ANNEE(+) and i.CODEBRAN=ex.CODEBRAN(+) and i.CODEINTE=ex.CODEINTE(+)  and i.NUMEPOLI=ex.NUMEPOLI(+)
 and  i.ANNEE=pa.ANNEE(+) and i.CODEBRAN=pa.CODEBRAN(+) and i.CODEINTE=pa.CODEINTE(+)  and i.NUMEPOLI=pa.NUMEPOLI(+)
 and  i.ANNEE=si.ANNEE(+) and i.CODEBRAN=si.CODEBRAN(+) and i.CODEINTE=si.CODEINTE(+)  and i.NUMEPOLI=si.NUMEPOLI(+)
 and  i.ANNEE=dts.ANNEE(+) and i.CODEBRAN=dts.CODEBRAN(+)and i.CODEINTE=dts.CODEINTE(+) and i.NUMEPOLI=dts.NUMEPOLI(+)
 and  i.ANNEE=dtc.ANNEE(+) and i.CODEBRAN=dtc.CODEBRAN(+)and i.CODEINTE=dtc.CODEINTE(+) and i.NUMEPOLI=dtc.NUMEPOLI(+)
 
 --)
 
 -- select*from actuary.DTM_ACTUARY where annee='2021'
 
 ;
 
 commit;
 
 end;
/
