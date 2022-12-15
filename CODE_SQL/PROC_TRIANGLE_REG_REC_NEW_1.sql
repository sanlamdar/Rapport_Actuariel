CREATE OR REPLACE PROCEDURE ACTUARY.PROC_TRIANGLE_REG_REC_NEW_1(month_id in varCHAR2,periode1 in number,periode2 in number,attri1 in number ,attri2 in number )  as
 
 ---attri1 : 0  et attri 2 =0  pour les non graves 
  ---attri1 : 1 et attri 2 =1  pour les  graves 
  ---attri1 : 1 et attri 2 =0 pour tout
 
begin

delete from actuary.triangle_inc_rec;
insert into actuary.triangle_inc_rec
--create table actuary.triangle_inc as
(
 ( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOTAL_REGLEMENT' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 to_char(datesurv,'yyyy') annee ,
                 datesurv,
                 dateeval,
                 to_char(dateeval,'yyyy')-  to_char(datesurv,'yyyy')+1 dev,
                 TOTAL_REGLEMENT value
                 FROM actuary.triangle_regle_recours  
                 where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy')>=periode1 and  (graves =attri1 or graves=attri2) )
                 group by annee, dev, branche1)                                
 union all 
 ( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOTAL_RECOURS' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 to_char(datesurv,'yyyy') annee,
                 datesurv,
                 dateeval,
                 to_char(dateeval,'yyyy')-  to_char(datesurv,'yyyy')+1 dev,
                 TOTAL_RECOURS value
                 FROM actuary.triangle_regle_recours  
                 where to_char(datesurv, 'yyyy') <=periode2 and to_char(datesurv,'yyyy')>=periode1 and  (graves =attri1 or graves=attri2) )
                    group by annee, dev, branche1)
union all
 ( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOT_REGLEMENT_NET' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 to_char(datesurv,'yyyy') annee,
                 datesurv,
                 dateeval,
                 to_char(dateeval,'yyyy')-  to_char(datesurv,'yyyy')+1 dev,
                 TOT_REGLEMENT_NET value
                 FROM actuary.triangle_regle_recours  
                 where to_char(datesurv, 'yyyy') <=periode2 and to_char(datesurv,'yyyy')>=periode1 and  (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1)
union all
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOTAL_REGLEMENT' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOTAL_REGLEMENT value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(dateeval,'q') trim_deal,
                        TOTAL_REGLEMENT
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)
union all
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOT_REGLEMENT_NET' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOT_REGLEMENT_NET value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(dateeval,'q') trim_deal,
                        TOT_REGLEMENT_NET
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)
union all
 (select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOTAL_RECOURS' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOTAL_RECOURS value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(dateeval,'q') trim_deal,
                        TOTAL_RECOURS
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)
union all
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOTAL_REGLEMENT' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOTAL_REGLEMENT value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') trim_surv,
                        to_char(dateeval,'mm') trim_deal,
                        TOTAL_REGLEMENT
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)
union all
 select * from (select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOT_REGLEMENT_NET' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOT_REGLEMENT_NET value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') trim_surv,
                        to_char(dateeval,'mm') trim_deal,
                        TOT_REGLEMENT_NET
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)
union all
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'TOTAL_RECOURS' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOTAL_RECOURS value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') trim_surv,
                        to_char(dateeval,'mm') trim_deal,
                        TOTAL_RECOURS
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)
union all
( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'ACQUISITION' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 to_char(date_acquisition, 'yyyy') annee,
                 date_emission,
                 date_acquisition,
                to_char(date_emission, 'yyyy')-to_char(date_acquisition, 'yyyy')+1 dev,
                 acquisition value
                 FROM actuary.triangle_exposition 
                 where  to_char(date_acquisition, 'yyyy')<= periode2 and  to_char(date_acquisition, 'yyyy')>=periode1)
                 group by annee, dev, branche1)
union all
 ( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'PRIME ACQUISE' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 to_char(date_acquisition, 'yyyy') annee,
                 date_emission,
                 date_acquisition,
                 to_char(date_emission,'yyyy')-to_char(date_acquisition,'yyyy')+1 dev,
                 prime_acquise value
                 FROM actuary.triangle_prime_acquise 
                 where to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy')>=periode1)
                 group by annee, dev, branche1)
union all
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'ACQUISITION' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    ACQUISITION value
                    from(
                        select
                        branche1,
                        to_char(date_emission,'yyyy') annee_deal,
                        to_char(date_acquisition,'yyyy') annee_surv,
                        to_char(date_emission, 'q') trim_deal,
                        to_char(date_acquisition,'q')  trim_surv,
                        ACQUISITION
                        from ACTUARY.triangle_exposition where  to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy') >=periode1))
    group by branche1, annee, dev)
union all
 (select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'ACQUISITION' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    ACQUISITION value
                    from(
                        select
                        branche1,
                        to_char(date_emission,'yyyy') annee_deal ,
                        to_char(date_acquisition,'yyyy') annee_surv,
                        to_char(date_emission, 'mm') trim_deal,
                        to_char(date_acquisition,'mm')  trim_surv,
                        ACQUISITION
                        from ACTUARY.triangle_exposition where to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy') >=periode1))
    group by branche1, annee, dev)
union all
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'PRIME ACQUISE' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    PRIME_ACQUISE value
                    from(
                        select
                        branche1,
                        to_char(date_emission,'yyyy') annee_deal,
                        to_char(date_acquisition,'yyyy')  annee_surv,
                        to_char(date_emission, 'q') trim_deal,
                        to_char(date_acquisition,'q')  trim_surv,
                        PRIME_ACQUISE
                        from ACTUARY.triangle_prime_acquise where to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy') >=periode1))
    group by branche1, annee, dev)
union all
 (select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'PRIME ACQUISE' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    PRIME_ACQUISE value
                    from(
                        select
                        branche1,
                        to_char(date_emission,'yyyy') annee_deal,
                        to_char(date_acquisition,'yyyy')  annee_surv,
                        to_char(date_emission, 'mm') trim_deal ,
                        to_char(date_acquisition,'mm') trim_surv,
                        PRIME_ACQUISE
                        from ACTUARY.triangle_prime_acquise where to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy') >=periode1))
    group by branche1, annee, dev)
union all  
    ( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'CHARGE' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 annee,
                 datesurv,
                 datedeal,
                 extract(year from datedeal)-  extract(year from datesurv)+1 dev,
                 CHARGE value
                 FROM actuary.tb_triangle_sinistre  
                 where annee <=periode2 and annee>=periode1 and  (graves =attri1 or graves=attri2) )
                    group by annee, dev, branche1)
                    
                    
 union all 
 
 ( select
                month_id month_id,
                branche1,
                annee,
                dev,
                count(distinct id_sini) value,
                'NBSIN' TRIANGLE, 
                'ANNEE' TRAITEMENT
                 from (
                 select
                 branche1,
                 annee,
                 datesurv,
                 datedeal,
                 to_char(datedeal,'yyyy')-  to_char(datesurv,'yyyy')+1 dev,
                    annee||'_'||codeinte||'_'||numesini   id_sini
                 FROM actuary.tb_triangle_sinistre  
                 where annee <=periode2 and annee>=periode1 and 
                 nbsin=1
                 and (graves =attri1 or graves=attri2)
                 )
                 group by annee, dev, branche1)
                    

union all 
 
 ( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'SAP_NET' TRIANGLE,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 annee,
                 datesurv,
                 datedeal,
                 extract(year from datedeal)-  extract(year from datesurv)+1 dev,
                 SAP_NET value
                 FROM actuary.tb_triangle_sinistre  
                 where annee <=periode2 and annee>=periode1 and  (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1)
                    
                    
union all 
 
( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'CHARGE_NET_RECOURS' TRIANGLE,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 annee,
                 datesurv,
                 datedeal,
                 extract(year from datedeal)-  extract(year from datesurv)+1 dev,
                 CHARGE_NET_RECOURS value
                 FROM actuary.tb_triangle_sinistre 
                 where annee <=periode2 and annee>=periode1 and (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1)
                    
                    
                    
 union all 
 
 ( select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'SAP' TRIANGLE,
                'ANNEE' TRAITEMENT 
                 from (select
                 branche1,
                 annee,
                 datesurv,
                 datedeal,
                 extract(year from datedeal)-  extract(year from datesurv)+1 dev,
                 SAP value
                 FROM actuary.tb_triangle_sinistre 
                 where annee <=periode2 and annee>=periode1 and (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1)    
                    
  union all 
 
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'CHARGE' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    CHARGE value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        CHARGE
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)
                    
                    
 union all 
 
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                 count(distinct id_sini) value,
                'NBSIN' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    id_sini 
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        annee||'_'||codeinte||'_'||numesini   id_sini
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 
                        and nbsin=1  
                        and  (graves =attri1 or graves=attri2)
                       
                        
                        ))
    group by branche1, annee, dev)
    
union all 
 
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'SAP_NET' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    SAP_NET value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        SAP_NET
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)                   
                    
union all 
 
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'CHARGE_NET_RECOURS' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    CHARGE_NET_RECOURS value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        CHARGE_NET_RECOURS
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)                   
                    
                    
 union all 
 
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'SAP' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    SAP value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        SAP
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1  and (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)                   
                    
                    
  union all 


(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'CHARGE' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    CHARGE value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        CHARGE
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)                   
                    
union all 
 
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                count(distinct id_sini) value,
                'NBSIN' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    id_sini 
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                         annee||'_'||codeinte||'_'||numesini   id_sini
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1 
                       and  nbsin=1  
                        and (graves =attri1 or graves=attri2)
                        
                    
                        ))
    group by branche1, annee, dev)                   

union all

(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'SAP_NET' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    SAP_NET value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        SAP_NET
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)                   
                    
union all 
 
(select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'CHARGE_NET_RECOURS' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    CHARGE_NET_RECOURS value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        CHARGE_NET_RECOURS
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)                   
                    
                    
 union all 
 
 (select
                month_id month_id,
                branche1,
                annee,
                dev,
                sum(value) value,
                'SAP' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    SAP value
                    from(
                        select
                        branche1,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        SAP
                        from ACTUARY.TB_TRIANGLE_SINISTRE where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev)                    
  );
    
 commit;   
    
-------------------------AVEC GARANTIE
    
    
       
delete from ACTUARY.TRIANGLE_INC_GARA;
insert into ACTUARY.TRIANGLE_INC_GARA
--create table actuary.triangle_inc as
(
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOTAL_REGLEMENT' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 codegara,
                 to_char(datesurv,'yyyy') annee ,
                 datesurv,
                 dateeval,
                 to_char(dateeval,'yyyy')-  to_char(datesurv,'yyyy')+1 dev,
                 TOTAL_REGLEMENT value
                 FROM ACTUARY.TRIANGLE_REGLE_RECOURS  
                 where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy')>=periode1 and  (graves =attri1 or graves=attri2) )
                 group by annee, dev, branche1, codegara)                                
 union all 
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOTAL_RECOURS' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                branche1,
                codegara,
                 to_char(datesurv,'yyyy') annee,
                 datesurv,
                 dateeval,
                 to_char(dateeval,'yyyy')-  to_char(datesurv,'yyyy')+1 dev,
                 TOTAL_RECOURS value
                 FROM actuary.triangle_regle_recours  
                 where to_char(datesurv, 'yyyy') <=periode2 and to_char(datesurv,'yyyy')>=periode1 and  (graves =attri1 or graves=attri2) )
                    group by annee, dev, branche1,codegara)
union all
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOT_REGLEMENT_NET' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 codegara,
                 to_char(datesurv,'yyyy') annee,
                 datesurv,
                 dateeval,
                 to_char(dateeval,'yyyy')-  to_char(datesurv,'yyyy')+1 dev,
                 TOT_REGLEMENT_NET value
                 FROM actuary.triangle_regle_recours  
                 where to_char(datesurv, 'yyyy') <=periode2 and to_char(datesurv,'yyyy')>=periode1 and  (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1, codegara)
union all
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOTAL_REGLEMENT' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOTAL_REGLEMENT value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(dateeval,'q') trim_deal,
                        TOTAL_REGLEMENT
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev, codegara)
union all
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOT_REGLEMENT_NET' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOT_REGLEMENT_NET value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(dateeval,'q') trim_deal,
                        TOT_REGLEMENT_NET
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev, codegara)
union all
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOTAL_RECOURS' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOTAL_RECOURS value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(dateeval,'q') trim_deal,
                        TOTAL_RECOURS
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev,codegara)
union all
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOTAL_REGLEMENT' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOTAL_REGLEMENT value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') trim_surv,
                        to_char(dateeval,'mm') trim_deal,
                        TOTAL_REGLEMENT
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev, codegara)
union all
 (select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOT_REGLEMENT_NET' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOT_REGLEMENT_NET value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') trim_surv,
                        to_char(dateeval,'mm') trim_deal,
                        TOT_REGLEMENT_NET
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev,codegara)
union all
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'TOTAL_RECOURS' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    TOTAL_RECOURS value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(dateeval,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') trim_surv,
                        to_char(dateeval,'mm') trim_deal,
                        TOTAL_RECOURS
                        from ACTUARY.triangle_regle_recours where to_char(datesurv,'yyyy') <=periode2 and to_char(datesurv,'yyyy') >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1, annee, dev, codegara)
union all
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'ACQUISITION' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 codegara,
                 to_char(date_acquisition, 'yyyy') annee,
                 date_emission,
                 date_acquisition,
                to_char(date_emission, 'yyyy')-to_char(date_acquisition, 'yyyy')+1 dev,
                 acquisition value
                 FROM ACTUARY.TRIANGLE_ACQUISE_GARA
                 where  to_char(date_acquisition, 'yyyy')<= periode2 and  to_char(date_acquisition, 'yyyy')>=periode1)
                 group by annee, dev, branche1, codegara)
union all
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'PRIME ACQUISE' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                branche1,
                codegara,
                 to_char(date_acquisition, 'yyyy') annee,
                 date_emission,
                 date_acquisition,
                 to_char(date_emission,'yyyy')-to_char(date_acquisition,'yyyy')+1 dev,
                 prime_acquise value
                 FROM ACTUARY.TRIANGLE_ACQUISE_GARA 
                 where to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy')>=periode1)
                 group by annee, dev, branche1,codegara)
union all
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'ACQUISITION' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    ACQUISITION value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(date_emission,'yyyy') annee_deal,
                        to_char(date_acquisition,'yyyy') annee_surv,
                        to_char(date_emission, 'q') trim_deal,
                        to_char(date_acquisition,'q')  trim_surv,
                        ACQUISITION
                        from ACTUARY.TRIANGLE_ACQUISE_GARA  where  to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy') >=periode1))
    group by branche1,codegara, annee, dev)
union all
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'ACQUISITION' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    ACQUISITION value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(date_emission,'yyyy') annee_deal ,
                        to_char(date_acquisition,'yyyy') annee_surv,
                        to_char(date_emission, 'mm') trim_deal,
                        to_char(date_acquisition,'mm')  trim_surv,
                        ACQUISITION
                        from ACTUARY.TRIANGLE_ACQUISE_GARA  where to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy') >=periode1))
    group by branche1,codegara, annee, dev)
union all
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'PRIME ACQUISE' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    PRIME_ACQUISE value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(date_emission,'yyyy') annee_deal,
                        to_char(date_acquisition,'yyyy')  annee_surv,
                        to_char(date_emission, 'q') trim_deal,
                        to_char(date_acquisition,'q')  trim_surv,
                        PRIME_ACQUISE
                        from ACTUARY.TRIANGLE_ACQUISE_GARA  where to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy') >=periode1))
    group by branche1,codegara, annee, dev)
union all
 (select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'PRIME ACQUISE' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    12*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    PRIME_ACQUISE value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(date_emission,'yyyy') annee_deal,
                        to_char(date_acquisition,'yyyy')  annee_surv,
                        to_char(date_emission, 'mm') trim_deal ,
                        to_char(date_acquisition,'mm') trim_surv,
                        PRIME_ACQUISE
                        from ACTUARY.TRIANGLE_ACQUISE_GARA  where to_char(date_acquisition,'yyyy') <=periode2 and to_char(date_acquisition,'yyyy') >=periode1))
    group by branche1,codegara, annee, dev)
    union all
    ( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'CHARGE' TRIANGLE ,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 codegara,
                 annee,
                 datesurv,
                 datedeal,
                 extract(year from datedeal)-  extract(year from datesurv)+1 dev,
                 CHARGE value
                 FROM ACTUARY.TB_TRIANGLE_SINISTRE_GARA  
                 where annee <=periode2 and annee>=periode1 and  (graves =attri1 or graves=attri2) )
                    group by annee, dev, branche1, codegara)
                    
                    
 union all 
 
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                count(distinct id_sini) value,
                'NBSIN' TRIANGLE, 
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 codegara,
                 annee,
                 datesurv,
                 datedeal,
                 to_char(datedeal, 'yyyy')-  to_char(datesurv,'yyyy')+1 dev,
                 annee||'_'||codeinte||'_'||numesini   id_sini
                 FROM actuary.tb_triangle_sinistre_gara  
                 where annee <=periode2 and annee>=periode1 and nbsin=1  
                   and (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1,codegara)

union all 
 
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'SAP_NET' TRIANGLE,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 codegara,
                 annee,
                 datesurv,
                 datedeal,
                 extract(year from datedeal)-  extract(year from datesurv)+1 dev,
                 SAP_NET value
                 FROM actuary.tb_triangle_sinistre_gara 
                 where annee <=periode2 and annee>=periode1 and  (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1,codegara)
                    
                    
union all 
 
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'CHARGE_NET_RECOURS' TRIANGLE,
                'ANNEE' TRAITEMENT
                 from (select
                 branche1,
                 codegara,
                 annee,
                 datesurv,
                 datedeal,
                 extract(year from datedeal)-  extract(year from datesurv)+1 dev,
                 CHARGE_NET_RECOURS value
                 FROM actuary.tb_triangle_sinistre_gara
                 where annee <=periode2 and annee>=periode1 and (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1,codegara)
                    
                    
                    
 union all 
 
( select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'SAP' TRIANGLE,
                'ANNEE' TRAITEMENT 
                 from (select
                 branche1,
                 codegara,
                 annee,
                 datesurv,
                 datedeal,
                 extract(year from datedeal)-  extract(year from datesurv)+1 dev,
                 SAP value
                 FROM actuary.tb_triangle_sinistre_gara
                 where annee <=periode2 and annee>=periode1 and (graves =attri1 or graves=attri2))
                    group by annee, dev, branche1,codegara)    
                    
  union all 
 
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'CHARGE' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    CHARGE value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        CHARGE
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1,codegara, annee, dev)
                    
                    
 union all 
 
(select
                month_id month_id,
               branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'NBSIN' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    NBSIN value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        NBSIN
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1,codegara, annee, dev)
    
union all 
 
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'SAP_NET' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    SAP_NET value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        SAP_NET
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1,codegara, annee, dev)                   
                    
union all 
 
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'CHARGE_NET_RECOURS' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    CHARGE_NET_RECOURS value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        CHARGE_NET_RECOURS
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1,codegara ,annee, dev)                   
                    
                    
 union all 
 
 (select
                month_id month_id,
               branche1,
               codegara,
                annee,
                dev,
                sum(value) value,
                'SAP' TRIANGLE,
                'TRIMESTRE' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||trim_surv annee,
                    4*(annee_deal- annee_surv) + (trim_deal-trim_surv)+ 1 dev,
                    SAP value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'q') trim_surv,
                        to_char(datedeal,'q') trim_deal,
                        SAP
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1  and (graves =attri1 or graves=attri2)))
    group by branche1,codegara, annee, dev)                   
                    
                    
  union all 


(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'CHARGE' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    CHARGE value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        CHARGE
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1,codegara, annee, dev)                   
                    
union all 
 
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'NBSIN' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    NBSIN value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        NBSIN
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and 
                        nbsin=1  
                         and (graves =attri1 or graves=attri2)))
    group by branche1,codegara,annee, dev)                   

union all

(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'SAP_NET' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    SAP_NET value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        SAP_NET
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and  (graves =attri1 or graves=attri2)))
    group by branche1,codegara, annee, dev)                   
                    
union all 
 
(select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'CHARGE_NET_RECOURS' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    CHARGE_NET_RECOURS value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        CHARGE_NET_RECOURS
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1,codegara, annee, dev)                   
                    
                    
 union all 
 
 (select
                month_id month_id,
                branche1,
                codegara,
                annee,
                dev,
                sum(value) value,
                'SAP' TRIANGLE,
                'MOIS' TRAITEMENT
                from(
                    select 
                    branche1,
                    codegara,
                    annee_surv||'-'||mois_surv annee,
                    12*(annee_deal- annee_surv) + (mois_deal-mois_surv)+ 1 dev,
                    SAP value
                    from(
                        select
                        branche1,
                        codegara,
                        to_char(datesurv,'yyyy') annee_surv,
                        to_char(datedeal,'yyyy') annee_deal,
                        to_char(datesurv, 'mm') mois_surv,
                        to_char(datedeal,'mm') mois_deal,
                        SAP
                        from ACTUARY.TB_TRIANGLE_SINISTRE_GARA where annee <=periode2 and annee >=periode1 and (graves =attri1 or graves=attri2)))
    group by branche1,codegara, annee, dev)
 );
commit;
delete from ACTUARY.TRIANGLE_INC_GARA2;
insert into ACTUARY.TRIANGLE_INC_GARA2 (
select
MONTH_ID,
BRANCHE1, 
i.CODEGARA,
LIBEGARA,
ANNEE,
DEV,
VALUE,
TRIANGLE,
TRAITEMENT
from
ACTUARY.TRIANGLE_INC_GARA i, ORASS_V6.REFERENCE_GARANTIE j
where i.codegara = j.codegara(+));
commit;
end;
/
commit;
