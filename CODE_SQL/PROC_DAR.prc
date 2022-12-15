CREATE OR REPLACE PROCEDURE ACTUARY.proc_DAR(month_id in varCHAR2,month_id_pred in varCHAR2, itt in int, fin in int, deal_date in varCHAR2,
 seuil_auto in int, seuil_incendie in int, seuil_rc in int, seuil_transport in int,fin_r in int) as

--seuil_auto=10000000, seuil_incendie=50000000, seuil_rc=30000000, seuil_tranport=20000000, 
---fin_r c'est l'annee de  fin de la boucle pour l'analyse de renouvellement
--deal_date ='20220930'
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


------------table de reglement-----------------------------
delete from  actuary.tb_reglement;
   
insert into actuary.tb_reglement
   
   SELECT c.CODEBRAN,b.LIBEBRAN,
       m.caterisq  ,c.libecate ,
       m.codegara , 
       d.LIBEGARA ,
       S.CODERISQ ,
       m.codeinte ,
       i.raisocin ,
       m.exersini ,
       m.numesini ,
       decode(m.natusini,'S','Maladie','M','Materiel','C','Corporel','D','Mixte (Mat. et Corp.)') natusini,         --m.natusini Nature, a faire  
       m.dateeval ,
       s.datesurv ,
       s.datedecl ,
       s.numepoli ,
       s.numeaven ,
       s.codeassu ,
       a.raissoci nom,
       -m.montprin montprin,
       -m.monthono monthono ,
       -(m.montprin + m.monthono) total_reglement ,ss.CODTYPSO ,TS.LIBTYPSO
  from v_mouvement_sinistre m,
  REFERENCE_GARANTIE d,
       sinistre s,
       categorie c,
       intermediaire i,
       assure a ,orass_v6.branche b 
        , sort_sinistre ss, type_sort ts
 where m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and m.CODEGARA=d.codegara
   and m.caterisq = c.codecate
   and m.codeinte = i.codeinte
   and s.codeassu = a.codeassu
   and m.natusini = s.natusini
   and m.typemouvement = 'REGLE'--renc
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
   and c.CODEBRAN=b.CODEBRAN
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                          and to_char(trunc(ss2.datsorsi),'YYYY') <=substr(month_id,1,4) )
   and ss.codtypso = ts.codtypso
   and to_char(m.dateeval,'YYYY') between  '2000' and substr(month_id,1,4)
  -- and  lower(a.raissoci) like '%;%'
 -- and m.caterisq between 400 and 412
 --  and (m.numepoli,m.intepoli) in (select police,inter from coassurance_detailles ) 
 --and s.codeassu in ()
 order by m.caterisq,
          m.exersini,
          m.codeinte,
          m.numesini,
          m.dateeval
          
          
;


---------------table de recours-------------------------------------------------
delete from  actuary.tb_recours;
   
 insert into actuary.tb_recours
   
  SELECT c.CODEBRAN,b.LIBEBRAN,
       m.caterisq  ,c.libecate ,
       m.codegara , 
       d.LIBEGARA ,
       S.CODERISQ ,
       m.codeinte ,
       i.raisocin ,
       m.exersini ,
       m.numesini ,
       decode(m.natusini,'S','Maladie','M','Materiel','C','Corporel','D','Mixte (Mat. et Corp.)') natusini,         --m.natusini Nature, a faire  
       m.dateeval ,
       s.datesurv ,
       s.datedecl ,
       s.numepoli ,
       s.numeaven ,
       s.codeassu ,
       a.raissoci nom,
       -m.montprin montprin,
       -m.monthono monthono ,
       -(m.montprin + m.monthono) total_reglement ,ss.CODTYPSO ,TS.LIBTYPSO
  from v_mouvement_sinistre m,
  REFERENCE_GARANTIE d,
       sinistre s,
       categorie c,
       intermediaire i,
       assure a ,orass_v6.branche b 
        , sort_sinistre ss, type_sort ts
 where m.codeinte = s.codeinte
   and m.exersini = s.exersini
   and m.numesini = s.numesini
   and m.CODEGARA=d.codegara
   and m.caterisq = c.codecate
   and m.codeinte = i.codeinte
   and s.codeassu = a.codeassu
   and m.natusini = s.natusini
   and m.typemouvement = 'RENC'--renc
   and m.codeinte = ss.codeinte
   and m.exersini = ss.exersini
   and m.numesini = ss.numesini
   and c.CODEBRAN=b.CODEBRAN
   and ss.datsorsi = (select max(ss2.datsorsi)
                        from sort_sinistre ss2
                       where ss2.codeinte = m.codeinte
                         and ss2.exersini = m.exersini
                         and ss2.numesini = m.numesini
                          and to_char(trunc(ss2.datsorsi),'YYYY') <=substr(month_id,1,4) )
   and ss.codtypso = ts.codtypso
   and to_char(m.dateeval,'YYYY') between  '2000' and substr(month_id,1,4)
  -- and  lower(a.raissoci) like '%;%'
 -- and m.caterisq between 400 and 412
 --  and (m.numepoli,m.intepoli) in (select police,inter from coassurance_detailles ) 
 --and s.codeassu in ()
 order by m.caterisq,
          m.exersini,
          m.codeinte,
          m.numesini,
          m.dateeval
          
          
 ;

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


--select*from actuary.tb_pn_risque_ref

---drop table actuary.tb_pn_risque_ref ;
delete from  actuary.tb_pn_risque_ref ;

insert into actuary.tb_pn_risque_ref 
--create table actuary.tb_pn_risque_ref  as 

(
select nvl(CODEBRAN,   0) CODEBRAN,
       nvl(BRANCHE1,   0) BRANCHE1,
       nvl(BRANCHE2,   0) BRANCHE2,
       nvl(NUM_POLICE, 0) NUM_POLICE, 
       nvl(NUMEPOLI,   0) NUMEPOLI,
       nvl(CODEINTE,   0) CODEINTE,
       nvl(CODERISQ, 0) CODERISQ, 
        IDPOLICE,MIN_YEAR,
       max(CODEASSU) CODEASSU,  
       max(NOM_ASSU) NOM_ASSU ,
       max(LIBTYPIN) LIBTYPIN,
       max(RAISOCIN) RAISOCIN,
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
CODEASSU,
NOM_ASSU,
LIBTYPIN,
RAISOCIN,
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

)
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
 
 --;
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
 
 --select count(*) from actuary.dtm_actuary1
  delete from actuary.dtm_actuary1;
  insert into actuary.dtm_actuary1
--drop table  actuary.dtm_actuary1
--create table actuary.dtm_actuary1  as (
 select
 
  tb.ANNEE,
  tb.CODEBRAN,
  ---GESTIONNAIRE,
  tb.CODEINTE,
  tb.NUMEPOLI,
  PRIME_EMISE,PRIMNETT_EMISSION,PARC_RISQUE,NB_AVENANT,ACCEQUIT_EMISE,COMISSION_EMISE,
  PRIME_EFFET,PRIMNETT_EFFET,ACCEQUIT_EFFET,COMISSION_EFFET,
   EXPOSITION,PRIME_ACQUISE,PRIMNETT_ACQUISE,
   SINPAY,SINPAY_ANT,RECENC,RECENC_ANT,SAP,AREC,CHARGE_SIN_SURVENANCE,NBSIN_SURVENANCE,CHARGE_SIN_SURVENANCE_GRAVE,NBSIN_SURVENANCE_GRAVE,
     SINPAY_GRAVE,SINPAY_ANT_GRAVE,RECENC_GRAVE,RECENC_ANT_GRAVE,SAP_GRAVE,AREC_GRAVE,CHARGE_SIN_SURVENANCE_BR,CHARGE_SIN_BR_GRAVE,
   CHARGE_SIN_EFFET, NBSIN_EFFET,
   CHARGE_DTSOINS,CHARGE_COMPTABLE,
   
   CHARGE_DTSOINS_ASCOMA,
  CHARGE_COMPTABLE_ASCOMA,
  
  CHARGE_DTSOINS_WTW,
  CHARGE_COMPTABLE_WTW,
  
  CHARGE_DTSOINS_MCI,
  CHARGE_COMPTABLE_MCI,
  
  CHARGE_DTSOINS_OLEA,
CHARGE_COMPTABLE_OLEA


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
   --union all select to_char(ANNEE_COMPTABLE) ANNEE, 81 CODEBRAN,to_char(nvl(GESTIONNAIRE,'4')) CODEINTE,to_char(nvl(NUMEPOLI,0))  NUMEPOLI from actuary.sinistre_health
  --union all select to_char(ANNEE_SURV) ANNEE, 81 CODEBRAN,to_char(nvl(GESTIONNAIRE,'4')) CODEINTE,to_char(nvl(NUMEPOLI,0))  NUMEPOLI from actuary.sinistre_health
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
    sum(SINPAY*STATUT_SIN_GRAVE) SINPAY_GRAVE,
    sum(SINPAY_ANT)                         SINPAY_ANT,
    sum(SINPAY_ANT*STATUT_SIN_GRAVE) SINPAY_ANT_GRAVE,
    
    sum(RECENC)                             RECENC,
    sum(RECENC*STATUT_SIN_GRAVE)          RECENC_GRAVE,
    sum(RECENC_ANT)                         RECENC_ANT,
    sum(RECENC_ANT*STATUT_SIN_GRAVE)          RECENC_ANT_GRAVE,
    sum(SAP)                                SAP,
    sum(SAP*STATUT_SIN_GRAVE)          SAP_GRAVE,
    sum(AREC)                               AREC,
    sum(AREC*STATUT_SIN_GRAVE)          AREC_GRAVE,
   
    
    sum(CHARGE_SIN_BR)                         CHARGE_SIN_SURVENANCE_BR,
    sum(CHARGE_SIN_BR*STATUT_SIN_GRAVE)          CHARGE_SIN_BR_GRAVE,
   
    sum(CHARGE_SIN)                         CHARGE_SIN_SURVENANCE,
    sum(CHARGE_SIN*STATUT_SIN_GRAVE) CHARGE_SIN_SURVENANCE_GRAVE,
    
   count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN_SURVENANCE,
    sum(STATUT_SIN_GRAVE) NBSIN_SURVENANCE_GRAVE
    
    
    from
    (
    select  ANNEE,CODEINTE,NUMEPOLI,NUMESINI,CODEBRAN,SINPAY,SINPAY_ANT,RECENC,RECENC_ANT,SAP, AREC, CHARGE_SIN,CHARGE_SIN_BR,
     case when CODEBRAN=4 and CHARGE_SIN>=seuil_auto then 1                                             --seuil_auto =10000000
     when (CODEBRAN=2 or CODEBRAN=3 or CODEBRAN=1)  and CHARGE_SIN>=seuil_incendie then 1               --seuil_incendie=50000000
     when CODEBRAN=7 and CHARGE_SIN>=seuil_rc then 1                                                    --seuil_rc=30000000
     when CODEBRAN in (51,52,53,54) and CHARGE_SIN>=seuil_transport then 1 else 0 END STATUT_SIN_GRAVE --seuil_transport=20000000
    FROM
    (
    select
    to_char(ANNEE) ANNEE,CODEINTE,NUMEPOLI,NUMESINI,CODEBRAN,
    sum(SINPAY) SINPAY,
    sum(SINPAY_ANT) SINPAY_ANT,
    sum(RECENC) RECENC,
    sum(RECENC_ANT) RECENC_ANT,
    sum(SAP) SAP,
    
    sum(AREC) AREC,
    sum(SINPAY)+ sum(SINPAY_ANT)+sum(SAP) CHARGE_SIN_BR,
    sum(SINPAY)+ sum(SINPAY_ANT)+sum(SAP)- (sum(RECENC)+sum(RECENC_ANT)+sum(AREC)  )  CHARGE_SIN
    
    --count(distinct ANNEE||'_'||CODEINTE||'_'||NUMESINI) NBSIN
    from
    
    (select*
    from
    actuary.CHARGE_SINISTRE)
    
    group by CODEINTE,to_char(ANNEE),NUMESINI,NUMEPOLI,CODEBRAN
    
    ) ) 
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
   
   
   --Ajouter le gestionnaire
   (select
ANNEE,
CODEBRAN,
CODEINTE,
NUMEPOLI,

sum(decode(GESTIONNAIRE,3002,CHARGE_DTSOINS_GEST,0)) CHARGE_DTSOINS_ASCOMA,
sum(decode(GESTIONNAIRE,3002,CHARGE_COMPTABLE_GEST,0)) CHARGE_COMPTABLE_ASCOMA,

sum(decode(GESTIONNAIRE,3003,CHARGE_DTSOINS_GEST,0)) CHARGE_DTSOINS_WTW,
sum(decode(GESTIONNAIRE,3003,CHARGE_COMPTABLE_GEST,0)) CHARGE_COMPTABLE_WTW,

sum(decode(GESTIONNAIRE,0,CHARGE_DTSOINS_GEST,0)) CHARGE_DTSOINS_MCI,
sum(decode(GESTIONNAIRE,0,CHARGE_COMPTABLE_GEST,0)) CHARGE_COMPTABLE_MCI,

sum(decode(GESTIONNAIRE,3918,CHARGE_DTSOINS_GEST,0)) CHARGE_DTSOINS_OLEA,
sum(decode(GESTIONNAIRE,3918,CHARGE_COMPTABLE_GEST,0)) CHARGE_COMPTABLE_OLEA


from
   (
   select distinct i.ANNEE,i.CODEBRAN,i.GESTIONNAIRE,i.CODEINTE,i.NUMEPOLI ,nvl(CHARGE_COMPTABLE_GEST,0)CHARGE_COMPTABLE_GEST
   ,nvl(CHARGE_DTSOINS_GEST,0) CHARGE_DTSOINS_GEST
   from 
   
   (
   select 
   ANNEE_COMPTABLE ANNEE, 81 CODEBRAN,
    nvl(CODEINTE,0) CODEINTE,
   nvl(GESTIONNAIRE,'AUCUN') GESTIONNAIRE,
    nvl(NUMEPOLI,0) NUMEPOLI
    from actuary.sinistre_health 
 union  
  select
   ANNEE_SURV ANNEE, 81 CODEBRAN,
    nvl(CODEINTE,0) CODEINTE,
    nvl(GESTIONNAIRE,'AUCUN') GESTIONNAIRE,
    nvl(NUMEPOLI,0) NUMEPOLI 
    from actuary.sinistre_health   
    )i,
    (
   select
   ANNEE_COMPTABLE ANNEE, 81 CODEBRAN,
    nvl(CODEINTE,0) CODEINTE,
   nvl(GESTIONNAIRE,'AUCUN') GESTIONNAIRE,
    nvl(NUMEPOLI,0) NUMEPOLI,
   sum(CHARGE) CHARGE_COMPTABLE_GEST--,
  -- sum(0) CHARGE_DTSOINS_GEST
 from actuary.sinistre_health group by ANNEE_COMPTABLE,
   nvl(GESTIONNAIRE,'AUCUN'),81,
    nvl(NUMEPOLI,0), nvl(CODEINTE,0) 
   )j,  
  
   (
   select
   ANNEE_SURV ANNEE, 81 CODEBRAN,
    nvl(CODEINTE,0) CODEINTE,
    nvl(GESTIONNAIRE,'AUCUN') GESTIONNAIRE,
    nvl(NUMEPOLI,0) NUMEPOLI,
  --  sum(0) CHARGE_COMPTABLE_GEST,
   sum(CHARGE) CHARGE_DTSOINS_GEST
   from actuary.sinistre_health group by ANNEE_SURV,
   nvl(GESTIONNAIRE,'AUCUN'),81,
    nvl(NUMEPOLI,0), nvl(CODEINTE,0)  ) k
    
    where i.ANNEE =j.ANNEE(+)
    and i.CODEBRAN =j.CODEBRAN(+)
    and i.GESTIONNAIRE =j.GESTIONNAIRE(+)
    and i.CODEINTE =j.CODEINTE(+)
    and i.NUMEPOLI =j.NUMEPOLI(+)
    
    and  i.ANNEE =k.ANNEE(+)
    and i.CODEBRAN =k.CODEBRAN(+)
    and i.GESTIONNAIRE =k.GESTIONNAIRE(+)
    and i.CODEINTE =k.CODEINTE(+)
    and i.NUMEPOLI =k.NUMEPOLI(+)
 ) 
 group by

ANNEE,
CODEBRAN,
CODEINTE,
NUMEPOLI
 
 
 )hea 
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
    -- and tb.ANNEE=cm.ANNEE(+)and tb.CODEBRAN=cm.CODEBRAN(+) and tb.CODEINTE=cm.CODEINTE(+) and tb.NUMEPOLI=cm.NUMEPOLI(+)
     and tb.ANNEE=hea.ANNEE(+)and tb.CODEBRAN=hea.CODEBRAN(+) and tb.CODEINTE=hea.CODEINTE(+) and tb.NUMEPOLI=hea.NUMEPOLI(+)


--)
;
commit;

  delete from actuary.dtm_actuary_p;  --where view_date=deal_date;
  
 -- select*from   actuary.dtm_actuary_p
  insert into actuary.dtm_actuary_p

--drop table actuary.dtm_actuary_p
-- create table actuary.dtm_actuary_p as (
 select
 
   i.ANNEE, 
 deal_date view_date,
-- '20220430'  view_date,
-- i.GESTIONNAIRE,
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
  nvl(SINPAY_GRAVE                   ,0) SINPAY_GRAVE               ,
   nvl(SINPAY_ANT_GRAVE               ,0) SINPAY_ANT_GRAVE             ,
   nvl(RECENC_GRAVE                     ,0) RECENC_GRAVE                 ,
   nvl(RECENC_ANT_GRAVE                 ,0) RECENC_ANT_GRAVE             ,
   nvl(SAP_GRAVE                        ,0) SAP_GRAVE                    ,
   nvl(AREC_GRAVE                       ,0) AREC_GRAVE                   ,
   nvl(CHARGE_SIN_BR_GRAVE    ,0) CHARGE_SIN_BR_GRAVE ,

 nvl(CHARGE_SIN_EFFET         ,0) CHARGE_SIN_EFFET     ,
   nvl(NBSIN_EFFET              ,0) NBSIN_EFFET          ,
   nvl(CHARGE_DTSOINS           ,0) CHARGE_DTSOINS       ,
   nvl(CHARGE_COMPTABLE         ,0) CHARGE_COMPTABLE     ,
    nvl(CHARGE_DTSOINS_ASCOMA,   0) CHARGE_DTSOINS_ASCOMA    ,
    nvl(CHARGE_COMPTABLE_ASCOMA, 0) CHARGE_COMPTABLE_ASCOMA  ,
    nvl(CHARGE_DTSOINS_WTW,      0) CHARGE_DTSOINS_WTW       ,
    nvl(CHARGE_COMPTABLE_WTW,    0) CHARGE_COMPTABLE_WTW     ,
    nvl(CHARGE_DTSOINS_MCI,      0) CHARGE_DTSOINS_MCI       ,
    nvl(CHARGE_COMPTABLE_MCI,    0) CHARGE_COMPTABLE_MCI     ,
    nvl(CHARGE_DTSOINS_OLEA,     0) CHARGE_DTSOINS_OLEA      ,
    nvl(CHARGE_COMPTABLE_OLEA,    0) CHARGE_COMPTABLE_OLEA
   
  
  
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

 ---table de croisement prime et sinistre par  police
delete from actuary.dtm_actuary ;
---where view_date=deal_date;
    
  insert into actuary.dtm_actuary
  
 -- drop table actuary.dtm_actuary
  --create table actuary.dtm_actuary as (
  
  select
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
        ---nvl(GESTIONNAIRE,4444) CODEGEST,
        ---case when GESTIONNAIRE=3003 then 'WTW' 
        ---     when GESTIONNAIRE=3002 then 'ASCOMA' 
        ---      when GESTIONNAIRE=3918 then 'OLEA' 
        ---       when GESTIONNAIRE=0 then 'MCI' else 'AUCUN' end LIBEGEST,
        i.CODEINTE                                                                    ,
         case when CODEINTEB is null then  CODEINTEBg else CODEINTEB end CODEINTEB         ,
        CODEASSU                                                                      ,
        CODEASSUB                                                                     ,
        NOM_ASSU                                                                      ,
        NOM_ASSUB   NOM_ASSU_ASSAINI                                                                  ,
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
        AREC                    ,
        
        SINPAY_GRAVE                                                                        ,
        SINPAY_ANT_GRAVE                                                                    ,
        RECENC_GRAVE                                                                        ,
        RECENC_ANT_GRAVE                                                                    ,
        SAP_GRAVE                                                                           ,
        AREC_GRAVE                                                                          ,
        CHARGE_SIN_BR_GRAVE               ,
        
        CHARGE_SIN_SURVENANCE CHARGE_SIN_MILLIARD ,
        case when CODEBRAN=81 then CHARGE_DTSOINS else CHARGE_SIN_SURVENANCE  end CHARGE_SIN_SURVENANCE                                                         ,
        case when CODEBRAN=81 then 0 else  NBSIN_SURVENANCE   end  NBSIN_SURVENANCE                                                          ,
         CHARGE_SIN_SURVENANCE_GRAVE ,
   NBSIN_SURVENANCE_GRAVE   ,
        CHARGE_SIN_EFFET                                                              ,
        NBSIN_EFFET                                                                   ,
        CHARGE_DTSOINS                                                                ,
        CHARGE_COMPTABLE                                                             ,
       CHARGE_DTSOINS_ASCOMA    ,
      CHARGE_COMPTABLE_ASCOMA  ,
      CHARGE_DTSOINS_WTW       ,
      CHARGE_COMPTABLE_WTW     ,
      CHARGE_DTSOINS_MCI       ,
      CHARGE_COMPTABLE_MCI     ,
      CHARGE_DTSOINS_OLEA      ,
       CHARGE_COMPTABLE_OLEA
       -- CHARGE_DTSOINS_GEST                                                           ,
       -- CHARGE_COMPTABLE_GEST

  from 
  (select*from actuary.dtm_actuary_p)i,
  (select 
   ANNEE,CODEINTE,
    max(CODEINTEB) CODEINTEBg,
    max(RAISOCIN) RAISOCINg,
   	max(RAISOCINB) RAISOCINBg,
   	max(LIBTYPIN) LIBTYPINg

  from actuary.dtm_actuary_p group by ANNEE,CODEINTE)j
where 
    i.CODEINTE=j.CODEINTE(+)
and i.ANNEE=j.ANNEE(+)
  
--)
;
 
 ---table de control 1
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
 sum(PRIME_ACQUISE) PRIME_ACQUISE, sum(PRIMNETT_ACQUISE) PRIMNETT_ACQUISE, sum(CHARGE_SIN_MILLIARD) CHARGE_SIN_SURVENANCE, 
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
 ---table de control 2
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
 sum(PRIME_ACQUISE) PRIME_ACQUISE, sum(PRIMNETT_ACQUISE) PRIMNETT_ACQUISE, sum(CHARGE_SIN_MILLIARD) CHARGE_SIN_SURVENANCE, 
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
 
 ---)
 
 -- select*from actuary.DTM_ACTUARY where annee='2021'
 
 ;
 
 
 
 
 --Production de tb_analyse_sp
 
 --drop table actuary.tb_analyse_sp;
 delete from actuary.tb_analyse_sp;
insert into ACTUARY.tb_analyse_sp  
---create table  ACTUARY.tb_analyse_sp   as 
(
select
CODEBRAN, nvl(LIBEBRAN1,0) BRANCHE1,
nvl(LIBEBRAN2,0) BRANCHE2,
ANNEE,
 sum(exposition        )   acquisition   ,
 sum(prime_acquise    )   prime_acquise  ,
 sum(primnett_acquise)  pnette_acquise  ,
 sum(parc_risque      )       nbrerisque  ,
 sum( NBSIN_SURVENANCE  )   NBSIN ,
 sum( SINPAY           ) + sum( SINPAY_ANT           )   TOT_REGLEMENT ,
 sum( SINPAY) + sum( SINPAY_ANT)-sum(RECENC) -sum(RECENC_ANT)   TOT_REGLEMENT_NET  ,
 sum(RECENC) +sum(RECENC_ANT)   RECOURS ,
 sum( SAP )   SAP  ,
 sum(SAP )-sum(AREC)   SAP_NET  ,
 sum( SINPAY) + sum( SINPAY_ANT)+ sum(SAP )  CHARGE  ,
 
sum( CHARGE_SIN_MILLIARD)    CHARGE_NET_RECOURS  ,
sum(CHARGE_DTSOINS)  CHARGE_DTSOINS,
 sum( NBSIN_SURVENANCE_GRAVE )   NBSIN_GRAVE  ,
 sum( SINPAY_GRAVE           ) + sum( SINPAY_ANT_GRAVE           )   TOT_REGLEMENT_GRAVES  ,
sum( SINPAY_GRAVE ) + sum( SINPAY_ANT_GRAVE )-sum(RECENC_GRAVE ) -sum(RECENC_ANT_GRAVE )   TOT_REGLEMENT_NET_GRAVES  ,
 sum(RECENC_GRAVE) +sum(RECENC_ANT_GRAVE)     RECOURS_GRAVES ,
 sum( SAP_GRAVE             )   SAP_GRAVES  ,
  sum(SAP_GRAVE )-sum(AREC_GRAVE)   SAP_NET_GRAVES  ,
 sum( SINPAY_GRAVE ) + sum( SINPAY_ANT_GRAVE )+ sum(SAP_GRAVE)    CHARGE_GRAVES ,
 sum( SINPAY_GRAVE ) + sum( SINPAY_ANT_GRAVE )+ sum(SAP_GRAVE) -(sum(RECENC_GRAVE) +sum(RECENC_ANT_GRAVE)+sum(AREC_GRAVE)) CHARGE_NET_RECOURS_GRAVES 


from actuary.dtm_actuary

group by
CODEBRAN, 
nvl(LIBEBRAN1 ,0),
nvl(LIBEBRAN2 ,0),
ANNEE

     
);
 
--Table garantie et risque
delete from  ACTUARY.PN_GARANTIE_RISQUE 
--where TO_CHAR (DATECOMP,'yyyymm')=month_id
;

insert into actuary.PN_GARANTIE_RISQUE  (
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
   CODEGARA,
   --NVL(NUMEAVEN,0) NUMEAVEN,
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
 where   TO_CHAR (DATECOMP,'yyyymm')>='1990'
  -- and codebran=4
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
 
 
);
  

delete from  ACTUARY.LAST_RISQ;
insert into  ACTUARY.LAST_RISQ
(
select
   i.CODEINTE,
   i.NUMEPOLI,
   i.CODERISQ,
   avenant_max,
   LIBERISQ,
   MARQVEHI,
   TYPEVEHI,
   NUMEIMMA,
   DATE_MEC,
   CARRVEHI,
   CODGENAU,
   CODEZONE,
   TYPEMOTE,
   NUMECHAS,
   PUISVEHI,
   POIDVEHI,
   CYLIVEHI,
   VITEVEHI,
   NOMBPLAC,
   CAPRIS01,
   CAPRIS02,
  NOMPLAIN,
   DATEENTR,
   DATESORT,
   CREE__LE,
   MODI_PAR,
   MODI__LE,
   CATERISQ

from
(select 
        CODEINTE,
        NUMEPOLI,
        CODERISQ,
      nvl( max(avenmodi),0) avenant_max
   from hist_risque
   
group by CODEINTE,
        NUMEPOLI,
        CODERISQ)i,   
(select *
 from hist_risque)j
        
 where i.CODEINTE=j.CODEINTE(+) 
and    i.NUMEPOLI=j.NUMEPOLI(+) 
and    i.CODERISQ=j.CODERISQ(+) 
and    i.avenant_max=nvl(j.avenmodi(+),0)

);  


delete from  PN_GAR_RISQUE;
insert into PN_GAR_RISQUE  

---create table PN_GAR_RISQUE as (select *from actuary.PN_AUTO_RISQUE)
(select 

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
   i.CODERISQ,
   CODEGARA,
   NUMEAVEN_G NUMEAVEN,
   avenmodi,
   avenant_max,
  -- NUMEAVEN_G NUMEAVEN,
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
      else nvl(primnett,0)/nvl(primnett1,0)*  CHIFAFFA_POLICE end CHIFAFFA,   
   case when  avenmodi is null then k.LIBERISQ   else j.LIBERISQ    end LIBERISQ, 
  case when  avenmodi is null  then k.MARQVEHI	 else j.MARQVEHI  end   MARQVEHI,
  case when  avenmodi is null  then k.TYPEVEHI	 else j.TYPEVEHI  end   TYPEVEHI,
  case when  avenmodi is null  then k.NUMEIMMA	 else j.NUMEIMMA  end   NUMEIMMA,
  case when  avenmodi is null  then k.DATE_MEC	 else j.DATE_MEC  end   DATE_MEC,
  case when  avenmodi is null  then k.CARRVEHI	 else j.CARRVEHI  end   CARRVEHI,
  case when  avenmodi is null  then k.CODGENAU	 else j.CODGENAU  end   CODGENAU,
  case when  avenmodi is null  then k.CODEZONE	 else j.CODEZONE  end   CODEZONE,
  case when  avenmodi is null  then k.TYPEMOTE	 else j.TYPEMOTE  end   TYPEMOTE,
  case when  avenmodi is null  then k.NUMECHAS	 else j.NUMECHAS  end   NUMECHAS,
  case when  avenmodi is null  then k.PUISVEHI	 else j.PUISVEHI  end   PUISVEHI,
  case when  avenmodi is null  then k.POIDVEHI	 else j.POIDVEHI  end   POIDVEHI,
  case when  avenmodi is null  then k.CYLIVEHI	 else j.CYLIVEHI  end   CYLIVEHI,
  case when  avenmodi is null  then k.VITEVEHI	 else j.VITEVEHI  end   VITEVEHI,
  case when  avenmodi is null  then k.NOMBPLAC	 else j.NOMBPLAC  end   NOMBPLAC,
  case when  avenmodi is null  then k.CAPRIS01	 else j.CAPRIS01  end   CAPRIS01,
  case when  avenmodi is null  then k.CAPRIS02	 else j.CAPRIS02  end   CAPRIS02,
  case when  avenmodi is null  then k.NOMPLAIN	 else j.NOMPLAIN  end   NOMPLAIN,
  case when  avenmodi is null  then k.DATEENTR	 else j.DATEENTR  end   DATEENTR,
  
  case when  avenmodi is null  then k.DATESORT	 else j.DATESORT  end   DATESORT,
  case when  avenmodi is null  then k.CREE__LE	 else j.CREE__LE  end   CREE__LE,
  case when  avenmodi is null  then k.MODI_PAR	 else j.MODI_PAR  end   MODI_PAR,
  case when  avenmodi is null  then k.MODI__LE	 else j.MODI__LE  end   MODI__LE,
  case when  avenmodi is null  then k.CATERISQ	 else j.CATERISQ  end   CATERISQ
--J.CODUSAAU
from

(select* 
from actuary.PN_GARANTIE_RISQUE) i,
(select CODEINTE,
NUMEPOLI,
CODERISQ,
nvl(avenmodi,0) avenmodi,
LIBERISQ,
MARQVEHI,
TYPEVEHI,
NUMEIMMA,
DATE_MEC,
CARRVEHI,
CODGENAU,
CODEZONE,
TYPEMOTE,
NUMECHAS,
PUISVEHI,
POIDVEHI,
CYLIVEHI,
VITEVEHI,
NOMBPLAC,
CAPRIS01,
CAPRIS02,
NOMPLAIN,
DATEENTR,
DATESORT,
CREE__LE,
MODI_PAR,
MODI__LE,
--CODEASSU,
CODUSAAU,
CATERISQ

from hist_risque) j,
(select CODEINTE,NUMEPOLI, CODERISQ,avenant_max,
LIBERISQ,
MARQVEHI,
TYPEVEHI,
NUMEIMMA,
DATE_MEC,
CARRVEHI,
CODGENAU,
CODEZONE,
TYPEMOTE,
NUMECHAS,
PUISVEHI,
POIDVEHI,
CYLIVEHI,
VITEVEHI,
NOMBPLAC,
CAPRIS01,
CAPRIS02,
NOMPLAIN,
DATEENTR,
DATESORT,
--CODUSAAU,
CREE__LE,
MODI_PAR,
MODI__LE,
CATERISQ

from  actuary.LAST_RISQ) k,

(select
   *from actuary.pn_all)h


where i.CODEINTE=j.CODEINTE(+) 
and   i.NUMEPOLI=j.NUMEPOLI(+)
and   i.CODERISQ=j.CODERISQ(+)
and   i.NUMEAVEN_G=j.avenmodi(+)

and   i.CODEINTE=k.CODEINTE(+) 
and   i.NUMEPOLI=k.NUMEPOLI(+)
and   i.CODERISQ=k.CODERISQ(+)


and   i.CODEINTE=h.CODEINTE(+) 
and   i.NUMEPOLI=h.NUMEPOLI(+) 
and   i.NUMEQUIT=h.NUMEQUIT(+) 
and   i.TYPEMOUV=h.TYPEMOUV(+) 
)
 
;



delete from tb_prime_ref_gara;  ---create table tb_prime_ref_gara as (select  *from tb_prime_ref_auto)
insert into tb_prime_ref_gara  (


select nvl(CODEBRAN,   0) CODEBRAN,
       nvl(BRANCHE1,   0) BRANCHE1,
       nvl(BRANCHE2,   0) BRANCHE2,
       nvl(NUM_POLICE, 0) NUM_POLICE, 
       nvl(NUMEPOLI,   0) NUMEPOLI,
       nvl(CODEINTE,   0) CODEINTE,
       nvl(CODERISQ, 0) CODERISQ, 
       nvl(CODEGARA,0) CODEGARA,
        IDPOLICE,MIN_YEAR,
       min(DATEEFFE)    DATEEFFE,
       min(datecomp)  datecomp,
                        DATEECHE,
       --sum(PRIMNETT2)    PRIMNETT_ALL,
       sum(PRIMNETCO2) PRIMNET,
       sum(CHIFAFFA)    CHIFAFFA
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
nvl(CODEINTE, 0)||nvl(NUMEPOLI, 0) NUM_POLICE,CODEGARA,
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
(select *from actuary.PN_GAR_RISQUE where  PRIMNETCO2<>0  )i,

--(select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto)h,
(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j,
(select EXTRACT(YEAR FROM min(DATEEFFE)) MIN_YEAR from actuary.PN_GAR_RISQUE )k
where i.CODEBRAN=j.CBR(+) 
---and nvl(i.CODEGARA,0)=h.VAR(+)

)
group by
nvl(CODEBRAN,   0)        ,
       nvl(BRANCHE1,   0) ,
       nvl(BRANCHE2,   0) ,
       nvl(NUM_POLICE, 0) , 
       nvl(NUMEPOLI,   0) ,
       nvl(CODEINTE,   0) ,
       nvl(CODERISQ, 0), nvl(CODEGARA,0),
       IDPOLICE,
       DATEECHE,MIN_YEAR
       
 ); 


---Prime et expo par annee d'acquisition sur l'auto


delete from  tb_prime_acq_agg_gara;
insert into tb_prime_acq_agg_gara 
(
select i.ANNEE, i.Annee_acquisition,
   j.CODEGARA,
   i.BRANCHE1,
   i.BRANCHE2,
   i.Acquisition,
   i.NbrePolice,
   j.Prime_acquise,
   j.Pnette_acquise
from

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEGARA,
   BRANCHE1,
   BRANCHE2,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   CODEGARA,
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
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992)) Acquisition,
   actuary.Exposition(DATEEFFE,DATEECHE) Expo,
   case when actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))>0 then 1 else 0 end kount,
   (case when actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))>0 then 1 else 0 end) *1 NbrePolice,
   1992 Annee_acquisition

from  actuary.tb_prime_ref_gara)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
   CODEGARA,
   BRANCHE1,
   BRANCHE2

) i,

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEGARA,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_acquise) Prime_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise--pn_risque1
from
(select
   datecomp,
   CODERISQ,
 --  VARRAPPORTACTUA 
   CODEGARA,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else CHIFAFFA *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))/Exposition(DATEEFFE,DATEECHE) end Prime_acquise,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else PRIMNETCO2 *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||1992) ,to_date('31/12/'||1992))/Exposition(DATEEFFE,DATEECHE) end Pnette_acquise,
    1992 Annee_acquisition

from  actuary.PN_GAR_RISQUE i
--, (select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto )h
---where nvl(i.CODEGARA,0)=h.VAR(+)
)
group by to_char(datecomp,'YYYY'), Annee_acquisition, CODEGARA) j
where i.ANNEE=j.ANNEE(+) and i.Annee_acquisition=j.Annee_acquisition(+) and
   i.CODEGARA=j.CODEGARA(+)

);




 FOR an IN (itt+1)..fin   loop

delete from  actuary.tb_prime_acq_agg_gara where annee_acquisition=an;
insert into  actuary.tb_prime_acq_agg_gara

   select i.ANNEE, i.Annee_acquisition,
   j.CODEGARA,
   i.BRANCHE1,
   i.BRANCHE2,
   i.Acquisition,
   i.NbrePolice,
   j.Prime_acquise,
   j.Pnette_acquise
from

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEGARA,
   BRANCHE1,
   BRANCHE2,
   --PRIME_CEDEE,
   sum(Acquisition) Acquisition,--tb_pn_risque_ref
   sum(NbrePolice) NbrePolice --tb_pn_risque_ref
from 
(select
   CODEGARA,
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
   --CODERISQ,
   --PRIME_CEDEE,
   --CHIFAFFA,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an)) Acquisition,
   actuary.Exposition(DATEEFFE,DATEECHE) Expo,
   case when actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end kount,
   (case when actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))>0 then 1 else 0 end) *1 NbrePolice,
   an Annee_acquisition

from  actuary.tb_prime_ref_gara)
group by to_char(datecomp,'YYYY'), Annee_acquisition,
   CODEGARA,
   BRANCHE1,
   BRANCHE2

) i,

(select to_char(datecomp,'YYYY') ANNEE, Annee_acquisition,
   CODEGARA,
   --BRANCHE1,
   --BRANCHE2,
   sum(Prime_acquise) Prime_acquise,--pn_risque1
   sum(Pnette_acquise) Pnette_acquise--pn_risque1
from
(select
   datecomp,
   CODERISQ,
 ---  VARRAPPORTACTUA 
   CODEGARA,
  -- COMMISSI,
  -- REGLCOMM,
  -- COMMISS_CEDEE,
  -- COMISS_NETTE_DE_COASS,
  -- COMMISSION_APPORTEUR,
  -- COMMGEST,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else CHIFAFFA *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Prime_acquise,
   case when actuary.Exposition(DATEEFFE,DATEECHE)=0 then 0 else PRIMNETCO2 *actuary.AnneePoliceAcquisition(DATEEFFE ,DATEECHE ,to_date('01/01/'||an) ,to_date('31/12/'||an))/Exposition(DATEEFFE,DATEECHE) end Pnette_acquise,
    an Annee_acquisition

from  actuary.PN_GAR_RISQUE i
--, (select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto )h
---where nvl(i.CODEGARA,0)=h.VAR(+)
)
group by to_char(datecomp,'YYYY'), Annee_acquisition, CODEGARA) j
where i.ANNEE=j.ANNEE(+) and i.Annee_acquisition=j.Annee_acquisition(+) and
   i.CODEGARA=j.CODEGARA(+);

--group by  i.ANNEE, i.Annee_acquisition,j.CODEBRAN, 
 --  i.BRANCHE1,
 --  i.BRANCHE2
END LOOP;






delete from  actuary.tb_sinistre_ref_GARa;  ---create table tb_sinistre_ref_GARa as (select  *from tb_sinistre_ref_auto)
insert into actuary.tb_sinistre_ref_GARa  (
select
CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEGARA,
count(*) NBSIN,
 sum(TOT_REGLEMENT) TOT_REGLEMENT,

sum(TOT_REGLEMENT_NET)  TOT_REGLEMENT_NET,
sum(RECOURS)  RECOURS,
sum(SAP) SAP,
sum(SAP_NET) SAP_NET,
sum(CHARGE) CHARGE, 
sum(CHARGE_NET_RECOURS)  CHARGE_NET_RECOURS,

sum(decode(graves,1,1,0)) NBSIN_GRAVE,
 sum(TOT_REGLEMENT*graves) TOT_REGLEMENT_GRAVES,

sum(TOT_REGLEMENT_NET*graves)  TOT_REGLEMENT_NET_GRAVES,
sum(RECOURS*graves)  RECOURS_GRAVES,
sum(SAP*graves) SAP_GRAVES,
sum(SAP_NET*graves) SAP_NET_GRAVES,
sum(CHARGE*graves) CHARGE_GRAVES, 
sum(CHARGE_NET_RECOURS*graves)  CHARGE_NET_RECOURS_GRAVES

from 
(

select  
CODEBRAN,BRANCHE1,
BRANCHE2,
  ANNEE,
CODEGARA,
case 
     when BRANCHE1='AUTO' and CHARGE_NET_RECOURS>=seuil_auto then 1
     when (BRANCHE1='DAB' or BRANCHE1='INCENDIE')  and CHARGE_NET_RECOURS>=seuil_incendie then 1 
     when BRANCHE1='RC' and CHARGE_NET_RECOURS>=seuil_rc then 1
     when BRANCHE1='TRANSPORT' and CHARGE_NET_RECOURS>=seuil_transport then 1 else 0 END graves,
TOT_REGLEMENT,
TOT_REGLEMENT_NET,
 RECOURS,
SAP,
SAP_NET,
CHARGE, 
CHARGE_NET_RECOURS


from

(select 
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI,
--VARRAPPORTACTUA 
CODEGARA,
sum(SINPAY)+sum(SINPAY_ANT)  TOT_REGLEMENT,

sum(SINPAY)+sum(SINPAY_ANT) -sum(RECENC)-sum(RECENC_ANT)  TOT_REGLEMENT_NET,
sum(RECENC)+sum(RECENC_ANT)  RECOURS,
sum(SAP) SAP,
sum(SAP) -sum(AREC) SAP_NET,
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) CHARGE, 
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) - (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_NET_RECOURS


from
(select*
from ACTUARY.CHARGE_SINISTRE where (SINPAY+SINPAY_ANT+SAP<>0 or SINPAY+SINPAY_ANT+SAP-RECENC-RECENC_ANT-AREC<>0)
and 
CODTYPSO in ('OU','RE','TR') and CODEINTE<>9999 )i,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j
---,(select nvl(VAR,0) VAR ,VARRAPPORTACTUA from actuary.TableGarantieAuto)h

where i.CODEBRAN=j.CBR(+) ---and nvl(i.codegara,0)=h.VAR(+)

group by 
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI,
CODEGARA
))
group by 
CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEGARA
);

--drop table actuary.tb_analyse_sp_gara;
delete from actuary.tb_analyse_sp_gara;   ---create table tb_analyse_sp_gara as (select  *from tb_analyse_sp_auto)
insert into actuary.tb_analyse_sp_gara  
--create table   actuary.tb_analyse_sp_gara  as
(
select
i.BRANCHE1,i.BRANCHE2,i.CODEGARA,
 i.ANNEE,
 nvl(acquisition ,             0)  acquisition   ,
 nvl(prime_acquise,            0)   prime_acquise  ,
 --nvl(cions,                    0)   cions  ,
 nvl(nbrepolice,               0)  nbrerisque   ,
 nvl( NBSIN,                   0)   NBSIN ,
 nvl( TOT_REGLEMENT,           0)   TOT_REGLEMENT ,
 nvl( TOT_REGLEMENT_NET,       0)   TOT_REGLEMENT_NET  ,
 nvl( RECOURS,                 0)    RECOURS ,
 nvl( SAP,                     0)   SAP  ,
 nvl(SAP_NET,                  0)   SAP_NET  ,
 nvl(CHARGE,                   0)   CHARGE  ,
 nvl( CHARGE_NET_RECOURS,      0)   CHARGE_NET_RECOURS  ,
 nvl( NBSIN_GRAVE,             0)   NBSIN_GRAVE  ,
 nvl(TOT_REGLEMENT_GRAVES,     0)   TOT_REGLEMENT_GRAVES  ,
 nvl(TOT_REGLEMENT_NET_GRAVES, 0)   TOT_REGLEMENT_NET_GRAVES  ,
 nvl(RECOURS_GRAVES,           0)    RECOURS_GRAVES ,
 nvl( SAP_GRAVES,              0)   SAP_GRAVES  ,
 nvl(SAP_NET_GRAVES,           0)   SAP_NET_GRAVES  ,
 nvl(CHARGE_GRAVES,            0)    CHARGE_GRAVES ,
 nvl(CHARGE_NET_RECOURS_GRAVES,0)    CHARGE_NET_RECOURS_GRAVES 


from 

(
select distinct to_char(ANNEE_ACQUISITION) ANNEE,CODEGARA,BRANCHE1,BRANCHE2 from  actuary.tb_prime_acq_agg_gara

union 

select distinct  ANNEE,CODEGARA,BRANCHE1,BRANCHE2 from  actuary.tb_sinistre_ref_GARa

)i,


( select 
ANNEE_ACQUISITION,CODEGARA,BRANCHE1,BRANCHE2,
sum(ACQUISITION) ACQUISITION,
sum(NBREPOLICE) nbrepolice,
sum(PRIME_ACQUISE) PRIME_ACQUISE,
sum(PNETTE_ACQUISE) PNETTE_ACQUISE
from  actuary.tb_prime_acq_agg_gara
group by ANNEE_ACQUISITION,CODEGARA,BRANCHE1,BRANCHE2 )k,
(select distinct *from  actuary.tb_sinistre_ref_GARa)j

where  i.CODEGARA=k.CODEGARA(+)
and    i.BRANCHE1=k.BRANCHE1(+)
      and i.ANNEE=k.ANNEE_ACQUISITION(+) 
      
and     i.CODEGARA=j.CODEGARA(+)
and    i.BRANCHE1=j.BRANCHE1(+)
      and i.ANNEE=j.ANNEE(+) 
     
);
 
commit;
-----------Analyse Renouvellement----------
-----------AUTO-------------------------------
---drop table actuary.tb_analyse_renouvellement ;


---insert into  ACTUARY.tb_analyse_renouvellement
------create table ACTUARY.tb_analyse_renouvellement as 
---
---(
---
---select i.*,
---
---case when  EXPOSITION_PRED>0 and EXPOSITION>0 then 'STABLE'
---     when  EXPOSITION_PRED=0 and EXPOSITION>0 and  EXPOSITION_WINBACK=0 then 'NOUVEAU'
---     when  EXPOSITION_PRED>0 and EXPOSITION=0 then 'NON_RENOUVELLE'
---     when  EXPOSITION_PRED=0 and EXPOSITION>0 and  EXPOSITION_WINBACK>0 then 'WINBACK' else 'OUT' end STATUT
---     
---from
---(
---select  
---        2011 ANNEE,
---        CODEINTE||NUMEPOLI NUM_POLICE,
---        NUMEPOLI,
---        CODEINTE,
---        max(LIBEBRAN1) LIBEBRAN1,
---        max(LIBEBRAN2) LIBEBRAN2,
---        max(CODEINTEB) CODEINTEB,
---        max(RAISOCIN_ASSAINI) RAISOCIN_ASSAINI,
---        max(NOM_ASSU) NOM_ASSU,
---        max(NOM_ASSU_ASSAINI) NOM_ASSU_ASSAINI,
---        max(CODECATE) CODECATE,
---        sum(decode(ANNEE,2010,PRIME_EMISE,0))                   PRIME_EMISE_PRED,
---        sum(decode(ANNEE,2010,COMISSION_EMISE,0))               COMISSION_EMISE_PRED,
---        sum(decode(ANNEE,2010,PRIME_ACQUISE,0))                 PRIME_ACQUISE_PRED,
---        sum(decode(ANNEE,2010,EXPOSITION,0))                    EXPOSITION_PRED,
---        sum(decode(ANNEE,2010,CHARGE_SIN_MILLIARD,0))           CHARGE_SIN_MILLIARD_PRED,
---        sum(decode(ANNEE,2010,CHARGE_SIN_SURVENANCE,0))         CHARGE_SIN_SURVENANCE_PRED,
---        sum(decode(ANNEE,2010,NBSIN_SURVENANCE,0))              NBSIN_SURVENANCE_PRED,
---        sum(decode(ANNEE,2010,NBSIN_SURVENANCE_GRAVE,0))        NBSIN_SURVENANCE_GRAVE_PRED,
---        sum(decode(ANNEE,2010,SINPAY,0))                        SINPAY_PRED,
---        sum(decode(ANNEE,2010,SINPAY_GRAVE,0))                  SINPAY_GRAVE_PRED,
---        sum(decode(ANNEE,2010,SINPAY_ANT,0))                    SINPAY_ANT_PRED,
---        sum(decode(ANNEE,2010,SINPAY_ANT_GRAVE,0))              SINPAY_ANT_GRAVE_PRED,
---        sum(decode(ANNEE,2010,RECENC,0))                        RECENC_PRED,
---        sum(decode(ANNEE,2010,RECENC_GRAVE,0))                  RECENC_GRAVE_PRED,
---        sum(decode(ANNEE,2010,RECENC_ANT,0))                    RECENC_ANT_PRED,
---        sum(decode(ANNEE,2010,RECENC_ANT_GRAVE,0))              RECENC_ANT_GRAVE_PRED,
---        sum(decode(ANNEE,2010,SAP,0))                           SAP_PRED,
---        sum(decode(ANNEE,2010,SAP_GRAVE,0))                     SAP_GRAVE_PRED,
---        sum(decode(ANNEE,2010,AREC,0))                          AREC_PRED,
---        sum(decode(ANNEE,2010,AREC_GRAVE,0))                    AREC_GRAVE_PRED,
---        
---        sum(decode(ANNEE,2011,PRIME_EMISE,0))                   PRIME_EMISE,
---        sum(decode(ANNEE,2011,COMISSION_EMISE,0))               COMISSION_EMISE,
---        sum(decode(ANNEE,2011,PRIME_ACQUISE,0))                 PRIME_ACQUISE,
---        sum(decode(ANNEE,2011,EXPOSITION,0))                    EXPOSITION,
---        sum(decode(ANNEE,2011,CHARGE_SIN_MILLIARD,0))           CHARGE_SIN_MILLIARD,
---        sum(decode(ANNEE,2011,CHARGE_SIN_SURVENANCE,0))         CHARGE_SIN_SURVENANCE,
---        sum(decode(ANNEE,2011,NBSIN_SURVENANCE,0))              NBSIN_SURVENANCE,
---        sum(decode(ANNEE,2011,NBSIN_SURVENANCE_GRAVE,0))        NBSIN_SURVENANCE_GRAVE,
---        sum(decode(ANNEE,2011,SINPAY,0))                        SINPAY,
---        sum(decode(ANNEE,2011,SINPAY_GRAVE,0))                  SINPAY_GRAVE,
---        sum(decode(ANNEE,2011,SINPAY_ANT,0))                    SINPAY_ANT,
---        sum(decode(ANNEE,2011,SINPAY_ANT_GRAVE,0))              SINPAY_ANT_GRAVE,
---        sum(decode(ANNEE,2011,RECENC,0))                        RECENC,
---        sum(decode(ANNEE,2011,RECENC_GRAVE,0))                  RECENC_GRAVE,
---        sum(decode(ANNEE,2011,RECENC_ANT,0))                    RECENC_ANT,
---        sum(decode(ANNEE,2011,RECENC_ANT_GRAVE,0))              RECENC_ANT_GRAVE,
---        sum(decode(ANNEE,2011,SAP,0))                           SAP,
---        sum(decode(ANNEE,2011,SAP_GRAVE,0))                     SAP_GRAVE,
---        sum(decode(ANNEE,2011,AREC,0))                          AREC,
---        sum(decode(ANNEE,2011,AREC_GRAVE,0))                    AREC_GRAVE
---        
---        
---        
--- 
---from dtm_actuary  where ANNEE<=2011
---
---group by 
---
--- 2011,
--- CODEINTE||NUMEPOLI ,
--- NUMEPOLI,
--- CODEINTE
---
---)i,
---(
---select distinct NUM_POLICE np,
---       EXPOSITION_WINBACK
---from tb_winback
---)j
---
---where i.NUM_POLICE=j.np(+)
---
---);
---




--drop table actuary.tb_winback ;


---



 for ab IN (2011+1)..fin_r   loop

--drop table ACTUARY.tb_winback;
delete from ACTUARY.tb_winback;
insert into  ACTUARY.tb_winback
---create table actuary.tb_winback as

(
select  
        
        CODEINTE||NUMEPOLI NUM_POLICE,
        NUMEPOLI,
        CODEINTE,
         max(exposition) exposition_WINBACK
        
from dtm_actuary  where ANNEE<ab

group by 
 CODEINTE||NUMEPOLI ,
        NUMEPOLI,
        CODEINTE

        
);

delete from ACTUARY.tb_analyse_renouvellement where annee=ab;
insert into  ACTUARY.tb_analyse_renouvellement
---create table ACTUARY.tb_analyse_renouvellement as 

(

select i.*,

case when  EXPOSITION_PRED>0 and EXPOSITION>0 then 'STABLE'
     when  EXPOSITION_PRED=0 and EXPOSITION>0 and  EXPOSITION_WINBACK=0 then 'NOUVEAU'
     when  EXPOSITION_PRED>0 and EXPOSITION=0 then 'NON_RENOUVELLE'
     when  EXPOSITION_PRED=0 and EXPOSITION>0 and  EXPOSITION_WINBACK>0 then 'WINBACK' else 'OUT' end STATUT
     
from
(
select  
        ab ANNEE,
        CODEINTE||NUMEPOLI NUM_POLICE,
        NUMEPOLI,
        CODEINTE,
        max(LIBEBRAN1) LIBEBRAN1,
        max(LIBEBRAN2) LIBEBRAN2,
        max(CODEINTEB) CODEINTEB,
        max(RAISOCIN_ASSAINI) RAISOCIN_ASSAINI,
        max(NOM_ASSU) NOM_ASSU,
        max(NOM_ASSU_ASSAINI) NOM_ASSU_ASSAINI,
        max(CODECATE) CODECATE,
        sum(decode(ANNEE,ab-1,PRIME_EMISE,0))                   PRIME_EMISE_PRED,
        sum(decode(ANNEE,ab-1,COMISSION_EMISE,0))               COMISSION_EMISE_PRED,
        sum(decode(ANNEE,ab-1,PRIME_ACQUISE,0))                 PRIME_ACQUISE_PRED,
        sum(decode(ANNEE,ab-1,EXPOSITION,0))                    EXPOSITION_PRED,
        sum(decode(ANNEE,ab-1,CHARGE_SIN_MILLIARD,0))           CHARGE_SIN_MILLIARD_PRED,
        sum(decode(ANNEE,ab-1,CHARGE_SIN_SURVENANCE,0))         CHARGE_SIN_SURVENANCE_PRED,
        sum(decode(ANNEE,ab-1,NBSIN_SURVENANCE,0))              NBSIN_SURVENANCE_PRED,
        sum(decode(ANNEE,ab-1,NBSIN_SURVENANCE_GRAVE,0))        NBSIN_SURVENANCE_GRAVE_PRED,
        sum(decode(ANNEE,ab-1,SINPAY,0))                        SINPAY_PRED,
        sum(decode(ANNEE,ab-1,SINPAY_GRAVE,0))                  SINPAY_GRAVE_PRED,
        sum(decode(ANNEE,ab-1,SINPAY_ANT,0))                    SINPAY_ANT_PRED,
        sum(decode(ANNEE,ab-1,SINPAY_ANT_GRAVE,0))              SINPAY_ANT_GRAVE_PRED,
        sum(decode(ANNEE,ab-1,RECENC,0))                        RECENC_PRED,
        sum(decode(ANNEE,ab-1,RECENC_GRAVE,0))                  RECENC_GRAVE_PRED,
        sum(decode(ANNEE,ab-1,RECENC_ANT,0))                    RECENC_ANT_PRED,
        sum(decode(ANNEE,ab-1,RECENC_ANT_GRAVE,0))              RECENC_ANT_GRAVE_PRED,
        sum(decode(ANNEE,ab-1,SAP,0))                           SAP_PRED,
        sum(decode(ANNEE,ab-1,SAP_GRAVE,0))                     SAP_GRAVE_PRED,
        sum(decode(ANNEE,ab-1,AREC,0))                          AREC_PRED,
        sum(decode(ANNEE,ab-1,AREC_GRAVE,0))                    AREC_GRAVE_PRED,
        
        sum(decode(ANNEE,ab,PRIME_EMISE,0))                   PRIME_EMISE,
        sum(decode(ANNEE,ab,COMISSION_EMISE,0))               COMISSION_EMISE,
        sum(decode(ANNEE,ab,PRIME_ACQUISE,0))                 PRIME_ACQUISE,
        sum(decode(ANNEE,ab,EXPOSITION,0))                    EXPOSITION,
        sum(decode(ANNEE,ab,CHARGE_SIN_MILLIARD,0))           CHARGE_SIN_MILLIARD,
        sum(decode(ANNEE,ab,CHARGE_SIN_SURVENANCE,0))         CHARGE_SIN_SURVENANCE,
        sum(decode(ANNEE,ab,NBSIN_SURVENANCE,0))              NBSIN_SURVENANCE,
        sum(decode(ANNEE,ab,NBSIN_SURVENANCE_GRAVE,0))        NBSIN_SURVENANCE_GRAVE,
        sum(decode(ANNEE,ab,SINPAY,0))                        SINPAY,
        sum(decode(ANNEE,ab,SINPAY_GRAVE,0))                  SINPAY_GRAVE,
        sum(decode(ANNEE,ab,SINPAY_ANT,0))                    SINPAY_ANT,
        sum(decode(ANNEE,ab,SINPAY_ANT_GRAVE,0))              SINPAY_ANT_GRAVE,
        sum(decode(ANNEE,ab,RECENC,0))                        RECENC,
        sum(decode(ANNEE,ab,RECENC_GRAVE,0))                  RECENC_GRAVE,
        sum(decode(ANNEE,ab,RECENC_ANT,0))                    RECENC_ANT,
        sum(decode(ANNEE,ab,RECENC_ANT_GRAVE,0))              RECENC_ANT_GRAVE,
        sum(decode(ANNEE,ab,SAP,0))                           SAP,
        sum(decode(ANNEE,ab,SAP_GRAVE,0))                     SAP_GRAVE,
        sum(decode(ANNEE,ab,AREC,0))                          AREC,
        sum(decode(ANNEE,ab,AREC_GRAVE,0))                    AREC_GRAVE
        
        
       
  
        
 
from dtm_actuary  where ANNEE>=ab-1 and ANNEE<=ab  

group by 

 ab,
 CODEINTE||NUMEPOLI ,
 NUMEPOLI,
 CODEINTE

)i,


(
select distinct NUM_POLICE np,
       EXPOSITION_WINBACK
from tb_winback
)j

where i.NUM_POLICE=j.np(+)

);

end loop;






-----TABLE COMPLETE DTM_ACTUARY_FULL
--drop table actuary.DTM_ACTUARY_FULL;
delete from actuary.DTM_ACTUARY_FULL;

insert into actuary.DTM_ACTUARY_FULL

--create table actuary.DTM_ACTUARY_FULL as

(
select

i.*,
PRIME_EMISE_PRED              ,
  COMISSION_EMISE_PRED        ,
  PRIME_ACQUISE_PRED          ,
  EXPOSITION_PRED             ,
  CHARGE_SIN_MILLIARD_PRED    ,
  CHARGE_SIN_SURVENANCE_PRED  ,
  NBSIN_SURVENANCE_PRED       ,
  NBSIN_SURVENANCE_GRAVE_PRED ,
  SINPAY_PRED                 ,
  SINPAY_GRAVE_PRED           ,
  SINPAY_ANT_PRED             ,
  SINPAY_ANT_GRAVE_PRED       ,
  RECENC_PRED                 ,
  RECENC_GRAVE_PRED           ,
  RECENC_ANT_PRED             ,
  RECENC_ANT_GRAVE_PRED       ,
  SAP_PRED                    ,
  SAP_GRAVE_PRED              ,
  AREC_PRED                   ,
  AREC_GRAVE_PRED             ,
  STATUT 
from

(
select

*from

ACTUARY.DTM_ACTUARY)i,

(select
* from ACTUARY.TB_ANALYSE_RENOUVELLEMENT)j

where

  i.annee=j.annee(+)
and i.codeinte=j.codeinte(+)
and i.numepoli=j.numepoli(+)



);

-------CONSTUCTION DES TRIANGLES DE DEVELOPPEMENT

---table exposition

--drop table actuary.triangle_exposition;
delete from actuary.triangle_exposition;

insert into actuary.triangle_exposition

--create table actuary.triangle_exposition as
(
select
i.*,
to_date(ANNEE||'1231','yyyymmdd')             DATE_EMISSION,
to_date(ANNEE_ACQUISITION||'1231','yyyymmdd') DATE_ACQUISITION
from actuary.tb_pn_risque_acq1 i

)
;




---table prime_acquisition

--drop table actuary.triangle_prime_acquise;
delete from actuary.triangle_prime_acquise;

insert into actuary.triangle_prime_acquise

--create table actuary.triangle_prime_acquise as
(
select
ANNEE               ,
ANNEE_ACQUISITION   ,
CODEBRAN            ,
BRANCHE1  ,
BRANCHE2  ,
CODEINTE            ,
NUMEPOLI            ,
PRIME_ACQUISE       ,
PNETTE_ACQUISE      ,

to_date(ANNEE||'1231','yyyymmdd')             DATE_EMISSION,
to_date(ANNEE_ACQUISITION||'1231','yyyymmdd') DATE_ACQUISITION
from actuary.tb_pn_risque_acq2 i,
(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j


where i.CODEBRAN=j.CBR(+)
)
;

---table triangle prime_acquisition et exposition par garantie

--drop table actuary.triangle_acquise_gara;
delete from actuary.triangle_acquise_gara;

insert into actuary.triangle_acquise_gara

--create table actuary.triangle_acquise_gara as
(
select
ANNEE                ,
ANNEE_ACQUISITION    ,
CODEGARA             ,
BRANCHE1             ,
BRANCHE2             ,
ACQUISITION          ,
NBREPOLICE           ,
PRIME_ACQUISE        ,
PNETTE_ACQUISE       ,

to_date(ANNEE||'1231','yyyymmdd')             DATE_EMISSION,
to_date(ANNEE_ACQUISITION||'1231','yyyymmdd') DATE_ACQUISITION
from actuary.tb_prime_acq_agg_gara 
)
;




--drop table actuary.tb_sinistre_grave;
delete from actuary.tb_sinistre_grave;

insert into actuary.tb_sinistre_grave

--create table actuary.tb_sinistre_grave as
(select  
CODEBRAN,BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
  NUMESINI,

TOT_REGLEMENT,
TOT_REGLEMENT_NET,
 RECOURS,
SAP,
SAP_NET,
CHARGE, 
CHARGE_NET_RECOURS,
case 
     when BRANCHE1='AUTO' and CHARGE_NET_RECOURS>=seuil_auto then 1
     when (BRANCHE1='DAB' or BRANCHE1='INCENDIE')  and CHARGE_NET_RECOURS>=seuil_incendie then 1 
     when BRANCHE1='RC' and CHARGE_NET_RECOURS>=seuil_rc then 1
     when BRANCHE1='TRANSPORT' and CHARGE_NET_RECOURS>=seuil_transport then 1 else 0 END graves,
CODTYPSO
from

(select 
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI,
CODTYPSO,

sum(SINPAY)+sum(SINPAY_ANT)  TOT_REGLEMENT,

sum(SINPAY)+sum(SINPAY_ANT) -sum(RECENC)-sum(RECENC_ANT)  TOT_REGLEMENT_NET,
sum(RECENC)+sum(RECENC_ANT)  RECOURS,
sum(SAP) SAP,
sum(SAP) -sum(AREC) SAP_NET,
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) CHARGE, 
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) - (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_NET_RECOURS


from
(select*
from actuary.charge_sinistre)i,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)j

where i.CODEBRAN=j.CBR(+)

group by 
CODTYPSO,
i.CODEBRAN,
BRANCHE1,
BRANCHE2,
  ANNEE,
CODEINTE,
NUMESINI
)
 where ((CODTYPSO='TR' and CHARGE_NET_RECOURS<>0)
 or CODTYPSO in ('OU','RE')) and CODEINTE<>9999
)
;

delete from actuary.TB_SINISTRE where substr(deal_date,1,4)=substr(month_id,1,4);

insert into actuary.TB_SINISTRE  (select*from actuary.charge_sinistre);



--drop table actuary.tb_triangle_sinistre;
delete from actuary.tb_triangle_sinistre;

insert into actuary.tb_triangle_sinistre

--create table actuary.tb_triangle_sinistre as

(

select
i.*,nvl(graves,0) graves,1 NBSIN, to_date(DEAL_DATE||'31','yyyymmdd') DATEDEAL,BRANCHE1,BRANCHE2
from

(select *from
(
select 
CODEBRAN,ANNEE,DATESURV,DATEDECL,CODEINTE,NUMESINI,DEAL_DATE,
sum(SINPAY)+sum(SINPAY_ANT)  TOT_REGLEMENT,
sum(SINPAY)+sum(SINPAY_ANT) -sum(RECENC)-sum(RECENC_ANT)  TOT_REGLEMENT_NET,
sum(RECENC)+sum(RECENC_ANT)  RECOURS,
sum(SAP) SAP,
sum(SAP) -sum(AREC) SAP_NET,
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) CHARGE, 
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) - (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_NET_RECOURS

from tb_sinistre 

group by CODEBRAN,ANNEE,DATESURV,DATEDECL,CODEINTE,NUMESINI,DEAL_DATE
)where CHARGE_NET_RECOURS<>0

)i,

(select annee an, codeinte co, numesini nu,graves from tb_sinistre_grave where graves=1)j,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)k

where i.CODEBRAN=k.CBR(+)
and i.annee=j.an(+)
and   i.codeinte=j.co(+)
and   i.numesini=j.nu(+)
);


-----triangle des sinistres sur toutes les vues d'inventaire par garantie

--drop table actuary.tb_triangle_sinistre_gara;
delete from actuary.tb_triangle_sinistre_gara;

insert into actuary.tb_triangle_sinistre_gara

--create table actuary.tb_triangle_sinistre_gara as

(

select
i.*,nvl(graves,0) graves,1 NBSIN, to_date(DEAL_DATE||'31','yyyymmdd') DATEDEAL,BRANCHE1,BRANCHE2
from

(select *from
(
select 
CODEBRAN,ANNEE,DATESURV,DATEDECL,CODEINTE,NUMESINI,DEAL_DATE,CODEGARA,
sum(SINPAY)+sum(SINPAY_ANT)  TOT_REGLEMENT,
sum(SINPAY)+sum(SINPAY_ANT) -sum(RECENC)-sum(RECENC_ANT)  TOT_REGLEMENT_NET,
sum(RECENC)+sum(RECENC_ANT)  RECOURS,
sum(SAP) SAP,
sum(SAP) -sum(AREC) SAP_NET,
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) CHARGE, 
sum(SINPAY)+sum(SINPAY_ANT)+sum(SAP) - (sum(RECENC)+sum(RECENC_ANT)+sum(AREC))  CHARGE_NET_RECOURS

from tb_sinistre 

group by CODEBRAN,ANNEE,DATESURV,DATEDECL,CODEINTE,NUMESINI,CODEGARA,DEAL_DATE
)where CHARGE_NET_RECOURS<>0

)i,

(select annee an, codeinte co, numesini nu,graves from tb_sinistre_grave where graves=1)j,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)k

where i.CODEBRAN=k.CBR(+)
and i.annee=j.an(+)
and   i.codeinte=j.co(+)
and   i.numesini=j.nu(+)
);
----triangle reglement-------------


--drop table actuary.triangle_regle_recours;
delete from actuary.triangle_regle_recours;
insert into actuary.triangle_regle_recours 

--create table actuary.triangle_regle_recours  as 

(
select distinct
i.codebran,BRANCHE1,BRANCHE2,exersini, codeinte,numesini,codegara,dateeval,datesurv,datedecl,TOTAL_REGLEMENT,TOTAL_RECOURS,TOT_REGLEMENT_NET,GRAVES
from

(
select distinct
   i.codebran,i.exersini, i.codeinte,i.numesini,i.codegara,i.dateeval,i.datesurv,i.datedecl,
   nvl(g.GRAVES,0) GRAVES,
   nvl(j.TOTAL_REGLEMENT,0) TOTAL_REGLEMENT, nvl(k.TOTAL_REGLEMENT,0) TOTAL_RECOURS,
nvl(j.TOTAL_REGLEMENT,0)-nvl(k.TOTAL_REGLEMENT,0) TOT_REGLEMENT_NET

from

(
select distinct * from
(select distinct codebran,exersini,codeinte,numesini,codegara,dateeval,datesurv,datedecl
 from actuary.tb_reglement  where CODTYPSO in ('OU','RE','TR') and CODEINTE not in (9999,9998,9995)
union all
select distinct codebran,exersini,codeinte,numesini,codegara,dateeval,datesurv,datedecl
from actuary.tb_recours where CODTYPSO in ('OU','RE','TR') and CODEINTE not in (9999,9998,9995)
)
)i,

(select codebran,exersini,codeinte,numesini,codegara,dateeval,datesurv,datedecl, sum(TOTAL_REGLEMENT) TOTAL_REGLEMENT
 from actuary.tb_reglement  where CODTYPSO in ('OU','RE','TR') and CODEINTE not in (9999,9998,9995)
 group by codebran,exersini,codeinte,numesini,codegara,dateeval,datesurv,datedecl )j,
 
(select codebran,exersini,codeinte,numesini,codegara,dateeval,datesurv,datedecl, sum(TOTAL_REGLEMENT) TOTAL_REGLEMENT
 from actuary.tb_recours  where CODTYPSO in ('OU','RE','TR') and CODEINTE not in (9999,9998,9995)
 group by codebran,exersini,codeinte,numesini,codegara,dateeval,datesurv,datedecl )k,
 
 (select distinct ANNEE,CODEINTE,NUMESINI,GRAVES from actuary.tb_triangle_sinistre)g
 
where i.exersini=j.exersini(+)
and   i.codeinte=j.codeinte(+)
and   i.numesini=j.numesini(+)
and   i.codegara=j.codegara(+)
and   i.codebran=j.codebran(+)
and   i.dateeval=j.dateeval(+)
and   i.datesurv=j.datesurv(+)
and   i.datedecl=j.datedecl(+)

and   i.exersini=k.exersini(+)
and   i.codeinte=k.codeinte(+)
and   i.numesini=k.numesini(+)
and   i.codegara=k.codegara(+)
and   i.codebran=k.codebran(+)
and   i.dateeval=k.dateeval(+)
and   i.datesurv=k.datesurv(+)
and   i.datedecl=k.datedecl(+)

and   i.exersini=g.annee(+)
and   i.codeinte=g.codeinte(+)
and   i.numesini=g.numesini(+)

)i,

(select CODEBRAN CBR,LIB1 BRANCHE1,LIB2 BRANCHE2 from actuary.branche)k

where i.CODEBRAN=k.CBR(+)

)

;


------resume des tables pour les triangles
--tb_triangle_sinistre  sinistre avec toutes les vues
--tb_triangle_sinistre_gara sinistre avec toutes les vues par garantie
--triangle_regle_recours    reglement brut, recours et reglement net, nb: il est déja à un niveau garantie
--triangle_exposition       donnees presentées par annee d'emission et d'acquisition
--triangle_prime_acquise  
--triangle_acquise_gara    exposition et prime acquise au niveau garantie




----Structure des contrats en portefeuille

--drop table actuary.tb_risque_renouvellement;

delete from actuary.tb_risque_renouvellement;


insert into  actuary.tb_risque_renouvellement
---create table actuary.tb_risque_renouvellement as

(
Select 
 CODEBRAN,
 BRANCHE1,
 BRANCHE2,
 CODEINTE,
 CODEINTE_ASSAINI,
 RAISOCIN,
 RAISOCIN_ASSAINI,
 NUMEPOLI,
 NUM_POLICE,
 CODEASSU,
 CODEASSU_ASSAINI,
 NOM_ASSU,
 NOM_ASSU_ASSAINI,
 LIBTYPIN, 
 MOIS_EFFET     ,
ANNEE_EFFET  ,

MOIS_ECHEANCE    ,
ANNEE_ECHEANCE  ,

MOIS_EMISSION     ,
ANNEE_EMISSION  ,

sum(PRIMNET) PRIME_NETTE,
sum(CHIFAFFA) CHIFAFFA,
sum(expo) EXPOSITION,
count(distinct CODERISQ) NB_RISQUE
 

 
from



(
select 

i.*,

  to_char(i.dateeffe,'MM')   MOIS_EFFET     ,
  to_char(i.dateeffe,'YYYY') ANNEE_EFFET  ,

  to_char(i.dateeche,'MM')   MOIS_ECHEANCE    ,
  to_char(i.dateeche,'YYYY') ANNEE_ECHEANCE  ,

  to_char(i.datecomp,'MM')   MOIS_EMISSION     ,
  to_char(i.datecomp,'YYYY') ANNEE_EMISSION  ,

  case when k.CODEASSUB is null then i.CODEASSU ELSE k.CODEASSUB  end CODEASSU_ASSAINI,
  case when k.NOMB is null then i.NOM_ASSU ELSE k.NOMB  end NOM_ASSU_ASSAINI,
 
  case when l.CODEINTEB is null then i.CODEINTE ELSE l.CODEINTEB  end CODEINTE_ASSAINI,
  case when l.RAISOCINB is null then i.RAISOCIN ELSE l.RAISOCINB  end RAISOCIN_ASSAINI

from

(select *from actuary.tb_pn_risque_ref)i,
(select*from actuary.tb_assu)k,
(select to_char(codeinte) codeinte,RAISOCIN,codeinteb,raisocinb from actuary.tb_inter)l

where
       i.CODEASSU=k.CODEASSU(+)
and    i.CODEINTE=l.CODEINTE(+)
)
group by
CODEBRAN,
 BRANCHE1,
 BRANCHE2,
 CODEINTE,
 CODEINTE_ASSAINI,
 RAISOCIN,
 RAISOCIN_ASSAINI,
 NUMEPOLI,
 NUM_POLICE,
 CODEASSU,
 CODEASSU_ASSAINI,
 NOM_ASSU,
 NOM_ASSU_ASSAINI,
 LIBTYPIN, 
 MOIS_EFFET     ,
ANNEE_EFFET  ,

MOIS_ECHEANCE    ,
ANNEE_ECHEANCE  ,

MOIS_EMISSION     ,
ANNEE_EMISSION 

)

;
 commit;
 
 end;
/
