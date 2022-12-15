# Libray and other -------------------------------------------------------------
library(tidyverse)

# Chargement de la base Dommage ------------------------------------------------
contrats = read.csv2("contrat.txt", sep = "|", encoding = "UTF-8", quote = "")
#' La base provient d'une extraction de la table Tb_Sinistre sur SQL avec les 
#' filtres suivants : deal_date = '202209'  and 
#'                    codebran = 4 and 
#'                    codtypso in ('TR', 'OU', 'RE') and 
#'                    couts_sinistres <> 0 and 
#'                    annee in (2021, 2022)

# Mise en forme ----------------------------------------------------------------
colnames(contrats)[1] = "ANNEE"
contrats = contrats[contrats$CODEGARA %in% c("TC", "TCL", "TCM", "TCP"),]
contrats = contrats %>% mutate(id = paste(CODEINTE, NUMEPOLI, sep = "_"),
                               garantie = "DOM")

# Calcul du cout de sinistre ---------------------------------------------------
agg = contrats %>% 
  group_by(ANNEE, garantie, id) %>% 
  summarise(cout_dom = sum(COUTS_SINISTRES, na.rm = TRUE)) %>% 
  filter(ANNEE == 2021)

# Chargement de la base des polices --------------------------------------------
valeurs = readxl::read_excel("AUTOmmm.xlsx")

# Mise en forme ----------------------------------------------------------------
valeurs$cout_gar.domm = agg$cout_dom[match(valeurs$ID, agg$id)]
valeurs$cout_gar.domm[is.na(valeurs$cout_gar.domm)] = 0
valeurs$prop_dom = ifelse(valeurs$CHSIN2021 == 0, 
                          0,
                          valeurs$cout_gar.domm / valeurs$CHSIN2021)
valeurs$Caterisque = substr(valeurs$ID, 1, 4)
valeurs$`SP 2022` = valeurs$CHSIN2022 / valeurs$PRIM2022
valeurs = valeurs %>% 
  select(ID, Caterisque, NOM_ASSU : STATUT_CONTRAT, `SP 2022`, cout_gar.domm, 
         prop_dom)

# Enregistrement de la base finale au format Excel -----------------------------
openxlsx::write.xlsx(valeurs, "MAUV_CONTRATs_AUTO_avec_gar_Dommage.xlsx")
