# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import numpy as np
from pyspark.sql import functions as F

# COMMAND ----------

def jdbc_connection_synapse(dbtable):
    url = "jdbc:sqlserver://globalxsegsrmdatafoundationdevsynapsemm-ondemand.sql.azuresynapse.net:1433;database=MW_GPD"
    user_name = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databricksusername"
    )
    password = dbutils.secrets.get(
        scope="mwoddasandbox1005devsecretscope", key="databrickspassword2"
    )
    df = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", dbtable)
        .option("user", user_name)
        .option("password", password)
        .option("authentication", "ActiveDirectoryPassword")
        .load()
    )
    return df

# COMMAND ----------


fact_performance = jdbc_connection_synapse('mm_test.vw_mw_gpd_fact_performance')
dim_campaign = jdbc_connection_synapse('mm_test.vw_mw_gpd_dim_campaign')
dim_creative = jdbc_connection_synapse('mm_test.vw_mw_gpd_dim_creative')
dim_mediabuy = jdbc_connection_synapse('mm_test.vw_mw_gpd_dim_mediabuy')
dim_country = jdbc_connection_synapse('mm_test.vw_mw_gpd_dim_country')
dim_channel = jdbc_connection_synapse('mm_test.vw_mw_gpd_dim_channel')
dim_product = jdbc_connection_synapse('mm_test.vw_mw_gpd_dim_product')


# COMMAND ----------

fact_performance1 = fact_performance.join(dim_country, on="country_id").withColumn("marketregion_code",
                   when(col('marketregion_code') != '', col('marketregion_code')).otherwise('NORTHAM')).join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id', how = 'left')
fact_performance1 = fact_performance1.filter(col("campaign_start_date") >= '2023-01-01T00:00:00.000+0000')

dim_campaign23 = dim_campaign.join(fact_performance1.select('gpd_campaign_id','marketregion_code','campaign_start_date','campaign_end_date','country_desc','platform_desc').distinct(), on='gpd_campaign_id')
dim_creative23 = dim_creative.join(fact_performance1.select('creative_id','marketregion_code','gpd_campaign_id','campaign_desc','campaign_start_date','campaign_end_date','country_desc','platform_desc').distinct(), on='creative_id')
dim_mediabuy23 = dim_mediabuy.join(fact_performance1.select('media_buy_id','marketregion_code','gpd_campaign_id','campaign_desc','campaign_start_date','campaign_end_date','country_desc','platform_desc').distinct(), on='media_buy_id')

# COMMAND ----------

fact_performance1.count()

# COMMAND ----------

fact_performance1.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setting the dictionaries with actual values

# COMMAND ----------

# MAGIC %md
# MAGIC #### Campaign dictionary

# COMMAND ----------


campaign_type_string = 'bid-adr, bid-dda, bid-nadr, bid-ibe, bid-adr-ibe, dir-adr, dir-dda, dir-nadr, dir-ibe, dir-adr-ibe'
campaign_type_list = [item.strip() for item in campaign_type_string.split(', ')]
campaign_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in campaign_type_list]


campaign_market_string = 'AE, AL, AR, AT, AU, AZ, BA, BE, BG, BH, BR, BY, CA, CH, CL, CN, CO, CR, CZ, DE, DK, DO, EC, EE, EG, ES, FI, FR, UK, GB, GE, GR, GT, Gulf, HK, HR, HU, ID, IE, IL, IN, IQ, IT, JO, JP, KE, KR, KSA, KW, KZ, LB, LT, LV, ME, MK, MX, MY, NL, NO, NZ, OM, PA, PE, PH, PL, PR, PT, QA, RO, RS, RU, SA, SE, SG, SI, SK, TH, TR, TW, UA, UG, US, VN, XK, YE, UZ, ZA'
campaign_market_list = [item.strip() for item in campaign_market_string.split(', ')]
campaign_market_list = [''.join(c for c in s if c.isalnum()).lower() for s in campaign_market_list]


campaign_subproduct_string = 'OIS, GM5, ADP, ADV, WAV, ALR, ALL, ANC, ALT, AMH, BAL, UBN, UBB, UBI, UBH, UBR, UBS, UBT, BAG, BIS, BIT, BLA, BVC, BOM, BOU, BRE, CSS, CAS, CAF, CAT, CEL, CES, CHD, CHA, CDP, CMB, CSH, CCV, COM, COJ, COD, COA, CRV, CRI, CPB, CSR, DCS, DTS, DIN, DOL, DDC, DMG, DMM, DSG, DGIC, DHC, DRE, EBL, EGM, EMT, ELL, ENT, ETM, EUK, ECD, ECG, ECM, EXE, EXG, EXM, EXR, EXP, FLU, FDP, FMB, FSP, FDS, FRE, FRO, GAL, GIT, GOL, GKS, GRN, GRS, GRF, GDA, HOO, HRS, HUB, ICT, ICD, IDG, ICE, INT, IWO, JWB, JEW, JFT, JFS, JUM, KAL, KAN, KAR, KBK, KBS, KSK, KBB, KFZ, KFB, KFP, KPB, KDS, KIT, KPF, KOK, LIG, LIM, LOC, LOO, LUB, LUC, MMS, MFB, MMM, MMX, MIC, MPB, MCC, MCB, MMSS, MVA, MAE, MAL, MGS, MAB, MAR, MAC, MFD, MSF, MFF, MET, MYW, MIL, MIR, MIS, MUB, MDG, NTC, NTD, NTM, NUD, NUT, OPT, OCE, ORA, ORO, ORG, ORM, ORC, ORP, PBT, PBD, PDG, PDA, PDD, PFP, PDW, PFF, PDP, PEM, PMB, PSH, CPK, POD, POT, PRM, RTB, RIP, RYC, RCS, ROY, SCH, SEA, SOC, SHE, SKT, SKP, SKG, SKH, SKW, SNI, SNP, SNN, SICC, SICPB, SNM, SNB, RNR, STB, STG, STM, SAG, SBS, STC, SUG, SUW, TAN, TAB, TEM, TOP, MU3, TCN, TRN, TRT, TUR, TWX, TCD, TCC, TWS, ULT, VPT, WAF, WHI, WHC, WHP, WHW, WIN, WDP, WMB, WSH, XBR, DTM, MMG, MPBG, MPBM, ORW, SKL, TMM, TTT, YES'



campaign_subproduct_list = [item.strip() for item in campaign_subproduct_string.split(', ')]
campaign_subproduct_list = [''.join(c for c in s if c.isalnum()).lower() for s in campaign_subproduct_list]

segment_string = 'pn, rc, Kind, inc, food, mw'
segment_list = [item.strip() for item in segment_string.split(', ')]
segment_list = [''.join(c for c in s if c.isalnum()).lower() for s in segment_list]

region_string = 'eur, na, mea, apac, latam'
region_list = [item.strip() for item in region_string.split(', ')]
region_list = [''.join(c for c in s if c.isalnum()).lower() for s in region_list]

portfolio_string = 'CT, DMM, CML, SPH, PH, FSH, CHO, GNM, FRC, RTH, MF-HS, MF-FS, TB, TB-FS, SOC, DRY, OTH, CT, DMM, CML, SPH, PH, FSH, CHO, CON, GUM, ICE, MIN, RTH, MF-HS, MF-FS, TB, TB-FS, SOC, DRY, OTH, care-treat, dog-mm, cat-mml, Ferret, Fish, choc, conf, ice-cream, mints, other, sub-snacks, dolmio, Care and treats, Cat Main Meal and Litter, Chocolate, Dog Main Meal, Dry, Fruity Confections, Gum/Mints, Masterfoods Foodservice, Masterfoods Herbs and Spices, Other, Ready to Heat, Seeds of Change, Super Premium Health, Tasty Bite, Tasty Bite Foodservice'

portfolio_list = [item.strip() for item in portfolio_string.split(', ')]
portfolio_list = [''.join(c for c in s if c.isalnum()).lower() for s in portfolio_list]

business_channel_string = 'DCOM, Sales, D2C, Brand'
business_channel_list = [item.strip() for item in business_channel_string.split(', ')]
business_channel_list = [''.join(c for c in s if c.isalnum()).lower() for s in business_channel_list]



media_channel_string = 'olv, ooh, ecom, affiliates, social, display, other, audio, print, influencer, tv, search, ppc, ctv'
media_channel_list = [item.strip() for item in media_channel_string.split(', ')]
media_channel_list = [''.join(c for c in s if c.isalnum()).lower() for s in media_channel_list]

media_objective_string = 'engagement, conversions, reach, views, app install, traffic, awareness, multiple, Leads  , awareness conversion'
media_objective_list = [item.strip() for item in media_objective_string.split(', ')]
media_objective_list = [''.join(c for c in s if c.isalnum()).lower() for s in media_objective_list]

start_string = '0622, 0223, 0922, 0126, 0426, 0824, 0424, 0925, 0324, 0423, 1125, 0822, 0422, 0123, 0624, 0726, 0725, 0224, 0225, 0326, 1027, 0527, 0222, 0723, 0325, 1227, 0923, 0924, 1026, 0127, 0322, 0523, 0727, 0927, 0226, 0122, 1222, 1024, 0625, 1226, 1123, 0724, 1122, 0827, 0825, 0522, 0722, 0323, 0524, 0926, 1022, 0427, 1126, 1023, 0227, 0425, 1124, 1127, 1025, 1225, 0526, 0525, 0826, 0124, 0623, 0823, 1224, 0627, 0125, 0626, 1223, 0327, 122, 123, 124, 125, 126, 127, 222, 223, 224, 225, 226, 227, 322, 323, 324, 325, 326, 327, 422, 423, 424, 425, 426, 427, 522, 523, 524, 525, 526, 527, 622, 623, 624, 625, 626, 627, 722, 723, 724, 725, 726, 727, 822, 823, 824, 825, 826, 827, 922, 923, 924, 925, 926, 927'


start_list = [item.strip() for item in start_string.split(', ')]
start_list = [''.join(c for c in s if c.isalnum()).lower() for s in start_list]

# COMMAND ----------

# dictionary
keys = ['campaign_type', 'campaign_market', 'campaign_subproduct', 'segment', 'region', 'portfolio', 'business_channel', 'media_channel', 'media_objective', 'starting_month']
values = [campaign_type_list, campaign_market_list, campaign_subproduct_list, segment_list, region_list, portfolio_list, business_channel_list, media_channel_list, media_objective_list, start_list]

campaign_dict = {k: v for k, v in zip(keys, values)}

# COMMAND ----------

campaign_dict['campaign_type']

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creative dictionary

# COMMAND ----------

creative_variant_string = 'v4, v2, v10, v5, v1, v8, v7, v9, v6, v3'
creative_variant_list = [item.strip() for item in creative_variant_string.split(', ')]
creative_variant_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_variant_list]

creative_type_string = 'static, gif, animated-banner, video, dynamic, carousel, leadgen, lens, filter, skins, text ad, shopping, audio'
creative_type_list = [item.strip() for item in creative_type_string.split(', ')]
creative_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_type_list]

ad_tag_size_string = 'none, 160x600, 300x250, 728x90, 1x1, 88x31, 120x20, 120x60, 120x90, 120x240, 120x600, 125x125, 168x28, 180x150, 216x36, 234x60, 240x400, 250x250, 300x50, 300x100, 300x600, 300x1050, 320x50, 320x240, 336x280, 468x60, 550x480, 640x480, 720x300, 970x90, 970x250, 1920x1080, 0x0, 180x180, 82x82, 210x185, 250x200, 250x50, 320x480, 580x80, 600x380, 960x600, 700x90, 970x66, 985x60, 1024x66, 1024x90, 1280x100, 976x66, 750x1174, 1600x320, 375x200, 1290x680, 414x125, 1712x136, 320x100, 768x66, 1248x702, 1608x850, 2560x2560, 300x115, 300x125, 402x596, 600x90, 480x320, 1280x970, 640x360, 1280x720, 640x100, 1024x768, 768x1024, 640x640, 630x920, 800x435'
ad_tag_size_list = [item.strip() for item in ad_tag_size_string.split(', ')]
ad_tag_size_list = [''.join(c for c in s if c.isalnum()).lower() for s in ad_tag_size_list]

dimension_string = '1280x100, 1600x320, 630x920, 300x250, 1290x680, 580x80, 5s, 8s, 320x100, 250x200, 19s, 300x115, 120x90, 16s, 300x50, 32s, 414x125, 168x28, 970x250, 51s, 23s, 750x1174, 728x90, 768x66, 1024x90, 216x36, 336x280, 120x240, 1280x970, 1248x702, 250x250, 480x320, 38s, 300x100, 4s, 34s, 18s, 35s, 700x90, 11s, 160x600, 9s, 320x240, 375x200, 45s, 43s, 720x300, 120x600, 82x82, 29s, 28s, 30s, 1x1, 320x480, 120x20, 640x360, 88x31, 640x480, 240x400, 7s, 985x60, 1920x1080, 22s, 970x66, 2560x2560, 60s, 15s, 600x90, 25s, 27s, 234x60, 1280x720, 768x1024, 180x150, 13s, 468x60, 64s, 10s, 640x640, 31s, 402x596, 36s, 120x60, 33s, 960REAL-TIME BIDDING (VIDEO)x600, 125x125, 800x435, 21s, 58s, 52s, 250x50, 14s, 300x1050, 30splus, 320x50, 640x100, 300x125, 6s, 970x90, 300x600, 1024x66, 0x0, 1024x768, 39s, 550x480, 976x66, none, 26s, 1712x136, 20s, 1608x850, 180x180, 40s, 24s, 37s, 17s, 600x380, 12s, 210x185'

dimension_list = [item.strip() for item in dimension_string.split(', ')]
dimension_list = [''.join(c for c in s if c.isalnum()).lower() for s in dimension_list]

cta_string = 'na, sign-up, learn-more, subscribe, get-directions, install-now, apply-now, send-message, get-offer, watch-more, call-now, use-app, contact, book-now, order-now, shop-now, download'
cta_list = [item.strip() for item in cta_string.split(', ')]
cta_list = [''.join(c for c in s if c.isalnum()).lower() for s in cta_list]

landing_page_string ='dm, biedronka, cdiscount, asda, social, kaufland, auchan, petsathome, carrefour, target, zooplus, lewiatan, eurocash, ms, Mikmak, aldi, delhaize, rewe, costco, fressnapf, intermarche, casino, amazon, allegro, boots, other, kroger, edeka, lidl, rossmann, sainsburys, mars, walmart, tesco, leclerc'
landing_page_list = [item.strip() for item in landing_page_string.split(', ')]
landing_page_list = [''.join(c for c in s if c.isalnum()).lower() for s in landing_page_list]

creative_market_string = 'AE, AL, AR, AT, AU, AZ, BA, BE, BG, BH, BR, BY, CA, CH, CL, CN, CO, CR, CZ, DE, DK, DO, EC, EE, EG, ES, FI, FR, UK, GB, GE, GR, GT, Gulf, HK, HR, HU, ID, IE, IL, IN, IQ, IT, JO, JP, KE, KR, KSA, KW, KZ, LB, LT, LV, ME, MK, MX, MY, NL, NO, NZ, OM, PA, PE, PH, PL, PR, PT, QA, RO, RS, RU, SA, SE, SG, SI, SK, TH, TR, TW, UA, UG, US, VN, XK, YE, UZ, ZA'
creative_market_list = [item.strip() for item in creative_market_string.split(', ')]
creative_market_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_market_list]

creative_subproduct_string = 'OIS, GM5, ADP, ADV, WAV, ALR, ALL, ANC, ALT, AMH, BAL, UBN, UBB, UBI, UBH, UBR, UBS, UBT, BAG, BIS, BIT, BLA, BVC, BOM, BOU, BRE, CSS, CAS, CAF, CAT, CEL, CES, CHD, CHA, CDP, CMB, CSH, CCV, COM, COJ, COD, COA, CRV, CRI, CPB, CSR, DCS, DTS, DIN, DOL, DDC, DMG, DMM, DSG, DGIC, DHC, DRE, EBL, EGM, EMT, ELL, ENT, ETM, EUK, ECD, ECG, ECM, EXE, EXG, EXM, EXR, EXP, FLU, FDP, FMB, FSP, FDS, FRE, FRO, GAL, GIT, GOL, GKS, GRN, GRS, GRF, GDA, HOO, HRS, HUB, ICT, ICD, IDG, ICE, INT, IWO, JWB, JEW, JFT, JFS, JUM, KAL, KAN, KAR, KBK, KBS, KSK, KBB, KFZ, KFB, KFP, KPB, KDS, KIT, KPF, KOK, LIG, LIM, LOC, LOO, LUB, LUC, MMS, MFB, MMM, MMX, MIC, MPB, MCC, MCB, MMSS, MVA, MAE, MAL, MGS, MAB, MAR, MAC, MFD, MSF, MFF, MET, MYW, MIL, MIR, MIS, MUB, MDG, NTC, NTD, NTM, NUD, NUT, OPT, OCE, ORA, ORO, ORG, ORM, ORC, ORP, PBT, PBD, PDG, PDA, PDD, PFP, PDW, PFF, PDP, PEM, PMB, PSH, CPK, POD, POT, PRM, RTB, RIP, RYC, RCS, ROY, SCH, SEA, SOC, SHE, SKT, SKP, SKG, SKH, SKW, SNI, SNP, SNN, SICC, SICPB, SNM, SNB, RNR, STB, STG, STM, SAG, SBS, STC, SUG, SUW, TAN, TAB, TEM, TOP, MU3, TCN, TRN, TRT, TUR, TWX, TCD, TCC, TWS, ULT, VPT, WAF, WHI, WHC, WHP, WHW, WIN, WDP, WMB, WSH, XBR, DTM, MMG, MPBG, MPBM, ORW, SKL, TMM, TTT'



creative_subproduct_list = [item.strip() for item in creative_subproduct_string.split(', ')]
creative_subproduct_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_subproduct_list]

creative_language_string = 'om, af, al, sq, am, ar, hy, as, az, eu, bn, bg, be, km, ca, zh, kw, hr, cs, da, nl, en, et, fo, fi, fr, gl, ka, de, el, kl, gu, ha, haw, he, iw, hi, hu, is, id, in, ga, it, ja, kn, kk, kok, ko, lv, lt, mk, ms, ml, mt, gv, mr, ne, no, nb, nn, or, ps, fa, pl, pt, pa, ro, ru, sr, sh, ii, si, sk, sl, so, es, sw, sv, gsw, ta, te, th, bo, ti, tr, uk, ur, uz, vi, cy, zu'
creative_language_list = [item.strip() for item in creative_language_string.split(', ')]
creative_language_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_language_list]

creative_platform_string = 'dv360, dv36o, meta, snap, amz, pin, tt, bing, google-ads, twitter, reddit, beeswax, linkedin, tradedesk, direct, ot'
creative_platform_list = [item.strip() for item in creative_platform_string.split(', ')]
creative_platform_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_platform_list]

creative_partner_string = '365, 9HO, ABC, ACA, AMG, ADC, ADL, ADM, ADT, ADPA, ADPI, ASW, AFB, AKI, ALG, AMZ, ARA, ANG, ASO, ATM, AVT, ASM, BME, BAO, BAU, BAV, BW, BTO, BIN, BLS, BLM, BLX, BRT, BRS, BUZ, CBS, CBA, CSP, CMS, CF, CIP, CMT, CLL, CLM, COM, CON, CRIT, CTC, CW, DLM, DV, DIO, DIS, DSN, DMS, DD, DV360, DV36O, EAS, EBA, ECO, EDAD, EDAM, eqt, ESPN, ETT, FCB, FBI, FND, FIN, FIT, FLA, fsq, FOX, FOR, FOD, GLO, GOA, GAAG, GOM, GOG, GYA, GDN, GOP, GPMD, GRE, GRM, grd, GRN, GRPM, GG, HM, HOL, HOT, HULU, HYB, IHR, imdb, IMG, IMHO, INCA, IND, INS, INM, IG, JG, KCL, KOL, KVT, LAD, LR, LKI, LIS, LIV, LM, MH, MAN, MRV, MAG, MAD, MAT, MAX, MBA, MDF, MER, MIQ, MIM, MIX, MOB, MOM, MOC, MUM, MXP, MYC, M6, MTF, MYT, NABD, NASCAR, NAV, N2KL, NBC, NBS, NCM, NW10, NC, NXD, NFL, NA, NRA, OAT, OGR, OKR, OLX, OPE, Other, PAN, PED, PTD, PET, PHA, PIN, PLS, PLT, PDC, PKT, PLM, PPS, PRI, PRF, QIY, qut, RDT, RZN, RCD, RNM, ROKU, RST, RTI, SWK, SAP, SBS, SEG, SEV, SHD, SMR, SNA, SID, SMRG, SPF, TBMO, TGT, TST, TEA, TCC, TNE, TEN, DOD, TTD, THRL, TIK, TPT, TRE, TUB, TUR, TVE, TVN, TWT, TWI, und, UNS, UNI, UNR, VM, val, VDX, VZN, VEV, VI, VIZ, VNT, VIU, VK, VOOT, VPO, WCO, wmg, WYK, WWE, XAX, XIA, XPL, XPA, YAH, YAN, YZL, YOU, YT, Z5, WBD, YELP'
creative_partner_list = [item.strip() for item in creative_partner_string.split(', ')]
creative_partner_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_partner_list]

creative_campaign_type_string = 'bid-adr, bid-dda, bid-nadr, bid-ibe, bid-adr-ibe, dir-adr, dir-dda, dir-nadr, dir-ibe, dir-adr-ibe'
creative_campaign_type_list = [item.strip() for item in creative_campaign_type_string.split(', ')]
creative_campaign_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_campaign_type_list]

creative_audience_type_string = 'demo-only, interest, behavioural, life-event, rtg, contextual, lal, keyword, weather, location, int-behav, int-kws, int-life, demo-int, demo-kws, behav-kws, behav-life, behav-location, FOB, HVC, Strategic, aim, lalp, pill, pilp, plal, plap'
creative_audience_type_list = [item.strip() for item in creative_audience_type_string.split(', ')]
creative_audience_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_audience_type_list]

creative_audience_desc_string = 'na, 2plus, 25+, 13-17, 13-24, 13-34, 13-44, 13-49, 13-54, 13plus, 15-17, 15-24, 15-34, 15-44, 15-54, 15plus, 18-24, 18-34, 18-39, 18-44, 18-45, 18-49, 18-54, 18plus, 20-55, 21plus, 22-25, 22-55, 25-34, 24-44, 25-49, 25-54, 25-64, 25-65plus, 35-44, 35-54, 35-65, 35plus, 45plus, 54plus, 65plus, m13-17, m13-24, m13-34, m13-44, m13-54, m13plus, m15-17, m15-24, m15-34, m15-44, m15-54, m15plus, m18-24, m18-34, m18-44, m18-54, m18plus, m25-34, m24-44, m25-54, m25-65plus, m35-44, m35-54, m35plus, m54plus, m65plus, f13-17, f13-24, f13-34, f13-44, f13-54, f13plus, f15-17, f15-24, f15-34, f15-44, f15-54, f15plus, f18-24, f18-34, f18-44, f18-49, f18-54, f18plus, F25-49, pbt, 21plus, F18-49, 13-49, f25-34, f24-44, f25-49, f25-54, f25-65plus, f35-44, f35-54, f35plus, f54plus, f65plus, web-view, web-purch, web-atc, web-com, web-cat, web-dog, web-cat-lal, web-dog-lal, web-lal, crm, shp, pla-vid, pla-eng, pla-com, mds-dog, mds-cat, mds-com, mds-d-lal, mds-c-lal, mds-dc-lal, par, pro, stu, gnz, mil, pms, hho-kids, hho-nokids, lat, lfe, wom, dat, gam, bak, sea, mus, spo, scr, haf, rmf, com, ora, sty, kmg, rfs, tlo, gif, fod, dtr, hvc, spo-nfl, spo-sup, par-out, mus-lat, haf-he, haf-flex, hf-pla, rec, wth, SNBDBE, SNBDSSW, SNBMDDB, SCC, SCHB, SCHE, SCHOL, SCHWRL, SCSL, SCYB, SCYE, yeq, yga, ysl, beq, cin, cnt, fdk, ntr, sea-fdy, sea-mdy, sea-vdy, sea-pri, wpkpb, wvi, ebb, lpb, cmp, ctb, pmb, adj, ACLAC, BDO, CA, CBFB, CBTB, CCFB, CCTB, CC, CI, CIAK, CK, CLNCOFCO, CO, CDI, DA, DBDFB, DBFB, DBTB, DBWFB, DCDFB, DCFB, DCK, DCTB, DCWFB, DI, DIAK, DK, DLNDOFDO, DO, dps, dhs, dr, dpa, daths, dfr, dt, dv, KIT, PI, PUI, PKI, PO, SDO, SDI, SDPI, VP, HI, gdacq01, fccbt01, fccbt02, fccbt03, fccbt04, fccbt05, fccbt06, fccbt07, fccbt08, fccbt09, fccbt10, fccbt11, fccbt12, fccbt13, fccbt14, fccbt15, fccbt16, fdcbt01, fdcbt02, fdcbt03, fdcbt04, fdcbt05, pccbt01, pccbt02, pccbt03, pccbt04, pccbt05, pdcbt06, pdcbt07, pdcbt08, pdcbt09, pdcbt10, pccbt11, pccbt12, pccbt13, fcchg01, fcchg02, fcchg03, fcchg04, fcchg05, pcchg01, pcchg02, pdchg03, pdchg04, pdchg05, pcchg06, pcchg07, pcchg08, pcchg09, gdcon01, fdenj01, fcenj01, fcenj02, fcenj03, fcenj04, fcenj05, pcenj01, pcenj02, pcenj03, gdeow01, gdeow02, gceow01, gceow02, gceow03, gceow04, gdevd01, gdevd02, gdevd03, gdevd04, gdevd05, gcevd01, gcevd02, gcevd03, gcevd04, gcevd05, gcevd06, gcevd07, gcevd08, gdevd06, gcevd09, gcevd10, gcevd11, gcevd12, gcevd13, gcevd14, gcevd15, gcevd16, gcevd17, fdfam01, fdfam02, fcfam01, gdfam01, gdfam02, gcfam01, pdftr01, pdftr02, pcftr03, pcftr04, pcftr05, pcftr06, pdftr07, pdftr08, pcftr09, pcftr10, pcftr11, pcftr12, pcftr13, pcftr14, pcftr15, fcgam01, fcgam02, fcgam03, gcgam01, fchea01, fchea02, fchea03, fdhea01, fdhea02, fdhea03, fdhea04, fdhea05, fdhea06, fchea04, fchea05, fchea06, fchea07, fchea08, fchea09, fchea10, fdhea07, fdhea08, fdhea09, fdhea10, fdhea11, fdhea12, fdhea13, fdhea14, fchea11, fchea12, fchea13, fchea14, fchea15, fchea16, fchea17, fdhea15, fdhea16, fdhea17, fdhea18, fdhea19, pchea01, pdhea02, pchea03, pchea04, pchea05, pchea06, pchea07, pdhea08, pdhea09, pchea10, pchea11, pchea12, pdhea13, pdhea14, pdhea15, pdhea16, pchea17, pchea18, pchea19, gdfam03, gdfam04, gdhea01, gchea01, fckit01, fckit02, fckit03, fckit04, fckit05, fckit06, fckit07, fckit08, fckit09, fckit10, fckit11, fckit12, fckit13, fckit14, fckit15, fckit16, fckit17, pckit01, pckit02, pckit03, pckit04, pckit05, pckit06, pckit07, pckit08, pckit09, gckit01, gckit02, gckit03, gckit04, gckit05, gckit06, gckit07, gckit08, gckit09, gckit10, gckit11, gckit12, gckit13, fclit01, fclit02, fclit03, pclit01, fclux01, fclux02, fclux03, fclux04, fdlux01, fdlux02, fdlux03, fdlux04, fclux05, fclux06, fclux07, gdhea02, gdhea03, gclux01, gclux02, gclux03, fdown01, fdown02, fcown01, fdown03, fdown04, fdown05, fcown02, fcown03, fcown04, fcown05, pdown01, pdown02, pcown03, gdlux01, gdlux02, gdlux03, gcown01, gcown02, gcown03, gcown04, gcown05, gcown06, gcper01, gcper02, fdpup01, fdpup02, fdpup03, pdpup01, pdpup02, pdpup03, pdpup04, pdpup05, gdown01, gdown02, gdown06, gdpup03, gdpup04, gdpup05, gdpup07, gdpup08, gdpup10, gdaff01, gdaff02, fdaff01, fdaff02, fdaff03, fdaff04, fdaff05, fcaff01, fcaff02, fcaff03, fcaff04, fcaff05, fcaff06, fcaff07, gcaff01, gcaff02, gcaff03, gcaff04, gcaff05, gcaff06, gcaff07, gcaff08, gcaff09, gcaff10, gdaff03, gdaff04, gdaff05, gdaff06, gdaff07, gdaff08, gdaff09, gdaff10, gdaff11, gdaff12, fcsus01, fcsus02, gcsus01, fcsus03, fcsus04, fdukc01, fddec01, fdukc02, fdukc, gdhea04, gdaff13, gcaff11, gcaff12, gcaff13, gdado01, gdado02, fdado01, gcaff14, gcaff15, gcaff16, gdevd07,FOB, HVC, Strategic, aim, behav-kws, behav-life, behav-location, behavioural, contextual, demo-int, demo-kws, demo-only, int-behav, int-kws, int-life, interest, keyword, lal, lalp, life-event, location, pill, pilp, plal, plap, rtg, weather, FOB'
creative_audience_desc_list = [item.strip() for item in creative_audience_desc_string.split(', ')]
creative_audience_desc_list = [''.join(c for c in s if c.isalnum()).lower() for s in creative_audience_desc_list]

# COMMAND ----------

# dictionary
keys = ['creative_variant','creative_type','ad_tag_size','dimension','cta','landing_page','creative_market','creative_subproduct','creative_language','creative_platform','creative_partner','creative_campaign_type','creative_audience_type','creative_audience_desc']
values = [creative_variant_list ,creative_type_list ,ad_tag_size_list ,dimension_list ,cta_list ,landing_page_list ,creative_market_list ,creative_subproduct_list ,creative_language_list ,creative_platform_list ,creative_partner_list ,creative_campaign_type_list ,creative_audience_type_list ,creative_audience_desc_list ]

creative_dict = {k: v for k, v in zip(keys, values)}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mediabuy dictionary

# COMMAND ----------

language_string = 'om, af, al, sq, am, ar, hy, as, az, eu, bn, bg, be, km, ca, zh, kw, hr, cs, da, nl, en, et, fo, fi, fr, gl, ka, de, el, kl, gu, ha, haw, he, iw, hi, hu, is, id, in, ga, it, ja, kn, kk, kok, ko, lv, lt, mk, ms, ml, mt, gv, mr, ne, no, nb, nn, or, ps, fa, pl, pt, pa, ro, ru, sr, sh, ii, si, sk, sl, so, es, sw, sv, gsw, ta, te, th, bo, ti, tr, uk, ur, uz, vi, cy, zu'
language_list = [item.strip() for item in language_string.split(', ')]
language_list = [''.join(c for c in s if c.isalnum()).lower() for s in language_list]

buying_type_string = 'auction, reserved, pg, pmp, ox, oth'
buying_type_list = [item.strip() for item in buying_type_string.split(', ')]
buying_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in buying_type_list]

costing_model_string = 'DCPV, View Through Cost Per Thousand, CPGS, Cost Per View, Dynamic Cost Per View, Dynamic Cost Per Thousand, Cost Per Engagement, Cost Per Acquisition, Cost Per Unique View, Other, CPI, CPRP, CPC, dCPIV, DCPC, CPE, CPCV, Cost Per Install, CPUL, Dynamic Cost Per Completed View, CPV, VCPM, Cost Per Click, Cost Per Thousand, CPU, FEE, Flat, Cost Per Reach Point, Dynamic Cost Per Incremental Visitor, AV, Cost Per Unit, CPA, Cost Per Lead, Added Value, Dynamic Cost Per Click, DCPM, CPM, Cost Per Completed View, CPL, Fee, Cost Per Viewable Thousand, dCPCV, VtCPM, Cost Per Games Session, Cost Per Games Sessionh'
costing_model_list = [item.strip() for item in costing_model_string.split(', ')]
costing_model_list = [''.join(c for c in s if c.isalnum()).lower() for s in costing_model_list]

mediabuy_campaign_type_string = 'bid-adr, bid-dda, bid-nadr, bid-ibe, bid-adr-ibe, dir-adr, dir-dda, dir-nadr, dir-ibe, dir-adr-ibe'
mediabuy_campaign_type_list = [item.strip() for item in mediabuy_campaign_type_string.split(', ')]
mediabuy_campaign_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_campaign_type_list]


audience_type_string = 'plal, int-kws, aim, location, lal, Strategic, lalp, life-event, crm, HVC, affinity, pbt, FOB, in-market, demo-kws, survey, retailer, pilp, plap, propensity, pill, contextual, demo-int, behav-kws, pbt-int-location, behavioural, pmp, interest, demo-only, keyword, behav-life, int-life, demo-pbt, rtg, weather, behav-location, int-behav'

audience_type_list = [item.strip() for item in audience_type_string.split(', ')]
audience_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in audience_type_list]

audience_desc_string = 'mds-cat, cdtp, fddec01, Lapsed-MMSB, sea, cin, gdaff13, 22-55, gckit06, ncer, DO, lfe, cs1u, OTGM, pdftr02, rec, LLGSC, gdown02, lpb, pter, fckit02, dob, 35-54, dr, Lapsed-SBCC, Extra-Ref-Con, fcenj05, gcevd16, web-com, pcftr06, pcd, CK, fccbt14, SKTGM, ntr, nc1a, SBB, CA, fcaff02, gclux01, gdaff01, fdhea14, fdhea13, NSSNC, SCYB, fckit05, ysl, fdhea01, CCFB, Music-Moments, CC, TS, fccbt06, fclit01, fdhea07, gceow02, gckit01, fdfam01, gcevd02, gcfam01, f25-54, pcchg07, gdpup03, dhs, fckit14, gdlux01, cptb, pdpup03, fcenj03, kk, rmf, fcsus03, Multic-AA, pchea06, pcchg02, DK, gcaff06, pchea04, sco, web-lal, Amp-Joy, fccbt09, doi, STBCC, vcwd, fclux05, GSCC, 15-54, f25-34, fcaff01, gcevd07, web-atc, gckit08, fdhea03, gdevd03, TWS, cdfb, gcown01, GenZ, gdown01, GatekeepersHVC, fcenj02, PBMsCB, Demo13-49, DCTB, 1plalm06, STBGC, haf, fccbt03, fdlux02, m13-54, PBFE, fcchg04, KIT, gp1a, gcevd11, m18-44, AS, f18plus, ser, f65plus, gdhea01, fdpup02, web-purch, DLNDOFDO, Late-Sea-HC, gcevd12, web-cat-lal, mds-com, FCP1P, gckit13, Demo13-39, fdukc, pdpup02, par-out, fdhea15, SM+DP, pckit07, tc1, pccbt11, GM, IM+Geo+RT, gcaff09, gdfam01, CCFE, fdcbt01, ssmc, pckit03, PARI, Sports-Fans, f18-34, gif, gcevd13, pdhea08, Kids-BDP, pcchg06, HCLBB, m18-24, CDM, fod, ebb, m13plus, gdado01, gdfam04, pchea07, fccbt13, Easter-CC, gcevd04, HI, gdlux03, beq, fcfam01, fdhea08, fcsus04, gckit03, pccbt05, gam, 1plalm08, pdpup05, gcaff14, HM+DP, Puppy, VP, gdpup04, HealthyCat, pt1u, SCM, STBGCC, pckit04, tdcdfb, web-dog, na, pchea12, fcenj01, gcaff04, pw1, gdaff06, fdown03, dt, fcown04, gcaff05, f15-17, SCSL, pdcbt07, Halloween-MBD, Induldge-Moments, fckit03, CLNCOFCO, fckit06, mds-d-lal, gcevd09, gdhea03, DBDFB, gckit04, F25-49, td1, 25-54, 15-34, BDO, m25-34, fdcbt02, SDPI, Mill-HVC, fccbt01, hcp, fdhea19, young-Moms, pper, 18-54, m18-54, pcw, gcaff07, fckit11, 20-55, gckit02, s1a, cetb, gnz, gcaff11, sea-mdy, Holiday-MBD, pchea18, HealthyDog, fcaff03, fdaff03, gdhea02, 1plalm03, m25-65plus, fdhea17, GHVC+BTSM, gdado02, tb, tdcd, ESHC, 18-34, NSM, SCCon, fchea13, tcct, gser, 13-24, 15-24, gcevd01, CBMD, tl1a, PSM, Multic-BCBNS, np1p, dpoi, dat, GenZ-HVC, f15-24, fcchg03, pro, sty, gdaff05, gcevd15, Halloween-CB, 65plus, Puppy1P, fdaff04, Gatekeepers, fdukc01, OCCC, tl1u, gchea01, dps, NFL-FCCC, 25-34, DA, GMD, pcftr14, HS, fchea02, fclit03, 18plus, Easter-ST, CAT, csbh, gdaff11, m15-17, Sports-Moments, SCHB, pcftr10, fccbt07, SNBDBE, FCP, LNSM+DP, dtr, mds-c-lal, 13-17, 18-39, pdftr01, pwcd, fclux01, fcown01, s1u, m15-54, pdchg05, kmg, pdhea02, CIAK, m35-54, Halloween-Mom, OWCC, fchea14, fccbt12, gcevd05, mus-lat, pchea03, tler, MMSECB, 13-49, epp, FAUD, gcaff03, pms, CCT, fchea09, usco, CFE, crm, 13-44, gmm, fclux07, gen-z-fashionistas, pdftr07, Snickers-Consumers, hho-nokids, spo, CFP, DHCC, web-cat, cool-parents, Ext-Con, LL5GC, Cat1P, gcown06, cdtb, Male-Gift-Givers, SCB, gs1, gcaff10, SDO, GenZ+PCM, gdacq01, daths, gcper01, fdown05, CSEM, Busy-Life, CCTB, gdown06, 18-45, fclux02, Pride-Moments, fckit13, OLLC, gdevd06, gckit07, NSSKC, gckit12, m15-34, pchea01, HBC, fcown02, pchea05, DIAK, gdevd04, pccbt12, Kitten1P, cmp, ap, NSSBC, fckit17, pla-vid, gdaff09, 1plalm09, fckit01, mil, f18-44, sea-pri, pcenj01, cwdo, SNG, fcenj04, Summer-moments, fccbt08, pcchg09, CPBCB, pcchg01, pppa, gcaff15, adj, gcaff13, fdlux01, FLG1P, RSB, fclux04, MDS+RT, USHCat, 25-64, pdftr08, nc1u, STBCCC, gb, ChOM, lctb, fchea16, fdhea16, PFMB, pcftr09, pdchg04, fdcbt05, f15-54, m13-44, Relax-moments, f15-34, focus-moments, gcaff02, Multic-AA-HVC, fckit10, fcaff04, 13plus, pdown01, HCCLTC, gc1u, gceow04, m18-34, yga, ora, Enter-Mom, gcown05, pdhea15, ki, fclux06, 1plalm02, fdukc02, DI, SKTCon, pdpup01, ELLC, fdown01, CFP1P, Twix-consumers, web-dog-lal, pb, fdown02, gcsus01, Holiday-Celeb, m35plus, pclit01, gdpup10, wom, pdown02, m13-34, 24-44, 18-24, celt, cwdl, 15plus, DDM, Easter-MBD, DCWFB, fckit09, haf-he, DBWFB, SNBMDDB, m35-44, twer, DCK, GenZ-Gamers, gcgam01, gdaff08, RCC, f24-44, fcchg05, fckit16, tdpcb, gcper02, hvc, 15-44, fcsus02, fccbt10, pckit08, FLG, coi, m15-24, 25-65plus, fdhea09, fdhea06, Holiday-SC, f18-54, SNBDSSW, f15-44, hf, DCDFB, gclux02, poi, ACLAC, cdgpb, Dog1P, gdfam02, f25-65plus, LLOWC, gceow01, TTM, rcw, 35-44, 21plus, gcevd08, fchea11, pcenj02, gdfam03, VDSC, f13plus, gcevd17, Late-Sea-Mom, m25-54, Kitten, bak, MDSC, fcgam02, fdaff05, MMSHCB, fcchg02, BSSCCC, f18-24, fdhea12, 35plus, fchea12, 35-65, fcaff07, gdaff07, pcftr15, pla-com, gckit10, sb, tt1, Demo13-49RT, gdeow02, SktCC, SC-GenZ, gdaff10, MMSBB, m15-44, pdcbt09, fchea15, gdcon01, pdhea09, tuw, ecet, fchea05, fdcbt03, yeq, fdhea10, gcown04, pncw, gdaff04, SCMRT, WCM, gper, dfr, pbt, pccbt01, fdaff02, pcftr05, m18plus, Young-Professionals, fclit02, gdaff12, BQ, pdpup04, sfb, GenZ+BM, cwdc, dpa, fcsus01, cwdf, pdcbt06, CO, 1plalm01, 1plalm04, gcaff16, lat, USH, fdhea02, hho-kids, GenZ-BCBNS, fchea04, fdhea05, HTT, Energy-DC, f25-49, tder, DCC, Cust-Scale, hf-pla, pdcbt10, haf-flex, SCHE, tk1, Y2K, cmp1, f54plus, fcgam01, CYOA, sea-fdy, TUP, fcaff05, gdaff02, SCC, gcevd03, pcftr11, m24-44, fccbt11, gcer, SDI, 13-34, CBFB, f13-24, pdhea14, pfb, hbs, 15-17, fckit08, gcevd14, DBFB, SCCC, fdcbt04, f35-44, fchea10, pe1a, cs1, HCCLSB, gdaff03, f13-54, fckit04, nper, sp1, pcftr03, Looking-my-best, fccbt05, gdpup08, tcer, HCCLSC, tuc, pccbt02, fdhea18, 25+, np1a, cfc, TDO, gdhea04, spo-nfl, gckit09, fccbt02, fchea06, fcaff06, Snacking-Moments, gcown02, fdenj01, tlo, pdcbt08, gckit05, East-Cel, DCFB, fccbt04, pccbt13, SCHOL, stu, Halloween-ST, m13-24, Feeling-my-best, tet, cdhc, 18-49, NLSM, fdpup03, td1u, college-prospects, PKI, pcenj03, RN5GC, f15plus, fchea17, sk1a, pckit01, fchea07, fckit12, PBMCB, mds-dog, pdcd, gceow03, wth, WW, fchea01, PUI, pckit02, fdhea04, shp, QV, pchea10, fckit07, fdown04, pmb, CI, DHCC1P, f35-54, GenZ-BTSM, gcevd06, ctb, m15plus, pcftr04, dv, YTS, fdhea11, PO, fdaff01, RTECB, fdfam02, pckit09, PBL, Multic-His-HVC, wvi, pt1a, Reach1P, gdevd01, Halloween-SB, Share-your-extra, ODE, Fitness-lovers, DBTB, ASP-GenZ, pcwd, pdchg03, com, pchea17, gcevd10, SCYE, gc1, spo-sup, 18-44, pckit05, f13-44, gcaff12, pdhea16, pckit06, FLDD, ppcb, par, tw1, web-view, m13-17, pcb, rfs, 13-54, SCHWRL, fccbt16, gdevd02, gdeow01, fdlux03, 45plus, gdlux02, pcown03, cepb, fdlux04, cnt, fcown03, cok, fckit15, f13-17, f18-49, Dove-CC, PI, m65plus, pp1, wpkpb, gdpup05, sktCCon, sea-vdy, 22-25, 54plus, LGTB, fccbt15, dok, pcdtb, pchea19, gpper, f35plus, 1plalm07, Celeb-Wom, cwcf, ESM, gckit11, CDI, gdpup07, 25-49, BTT, mus, pcftr13, gcaff08, pcftr12, pla-eng, cb, pccbt04, cdsb, gcaff01, gdevd05, scr, fcown05, fdk, fcgam03, fdpup01, gcown03, pccbt03, CBTB, F18-49, gpp1, mds-dc-lal, LLMMsB, fcchg01, fchea03, HE, TTGT, fchea08, m54plus, TMS, NPCBHII, Baking-Moments, Multic-His, gclux03, pcdd, pdhea13, pcchg08, tter, pchea11, 2plus, cdfg, SBM+DP, gdevd07, MMSHDCB, cdpfb, 1plalm05, f13-34, OCC, fclux03, pwer, fdado01'


audience_desc_list = [item.strip() for item in audience_desc_string.split(', ')]
audience_desc_list = [''.join(c for c in s if c.isalnum()).lower() for s in audience_desc_list]

data_type_string = 'Multiple, 1pd, 2pd, NA, 3pd'
data_type_list = [item.strip() for item in data_type_string.split(', ')]
data_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in data_type_list]

optimisation_string = 'reach, downloads, ad-recall, views, eng, clicks, lpv, atc, purchases, leads, catalog, roas, Ad Recall, Add To Cart, Catalog Sales, Downloads, Engagement, Landing Page Views, Leads, Outbound Clicks, Purchases, Reach, Return on Ad Spend, Views'


optimisation_list = [item.strip() for item in optimisation_string.split(', ')]
optimisation_list = [''.join(c for c in s if c.isalnum()).lower() for s in optimisation_list]

placement_type_string= 'bumpers, conversation, AR-Lens, camera, commercials, discovery, feed, hybrid, instream, Multi, newsfeed, non-skip, other, pmax, post, preroll, reels, shopping, shorts, skippable, sponsored-brand, sponsored-display, sponsored-product, stories, text ads, trueview'
placement_type_list = [item.strip() for item in placement_type_string.split(', ')]
placement_type_list = [''.join(c for c in s if c.isalnum()).lower() for s in placement_type_list]

mediabuy_format_string = 'animated banner, audio, camera, dynamic, static, hybrid, text ad, carousel, skins, video, other'
mediabuy_format_list = [item.strip() for item in mediabuy_format_string.split(', ')]
mediabuy_format_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_format_list]

device_string = 'Mobile, Desktop, Cross Device, Connected TV, Digital OOH, E-mails, Gaming Console, Mobile in app, Mobile Web, Multiplatform, Tablet, Tablet in app, Tablet Web, Wearables'
device_list = [item.strip() for item in device_string.split(', ')]
device_list = [''.join(c for c in s if c.isalnum()).lower() for s in device_list]

mediabuy_ad_tag_size_string = 'na, 0x0, 1024x66, 1024x768, 1024x90, 120x20, 120x240, 120x60, 120x600, 120x90, 1248x702, 125x125, 1280x100, 1280x720, 1280x970, 1290x680, 1456x180, 1600x320, 1608x850, 160x600, 168x28, 1712x136, 180x150, 180x180, 1920x1080, 1x1, 210x185, 216x36, 234x60, 240x400, 250x200, 250x250, 250x50, 2560x2560, 300x100, 300x1050, 300x115, 300x125, 300x250, 300x50, 300x600, 320x100, 320x240, 320x480, 320x50, 336x280, 375x200, 402x596, 414x125, 468x60, 480x320, 550x480, 580x80, 600x380, 600x90, 600x500, 630x920, 640x100, 640x360, 640x480, 640x640, 700x90, 720x300, 728x90, 750x1174, 768x1024, 768x66, 800x435, 82x82, 88x31, 960x600, 970x250, 970x66, 970x90, 976x66, 985x60, 1200x1200, 1400x720, 1440x720, 1920x1440, 200x200'

mediabuy_ad_tag_size_list = [item.strip() for item in mediabuy_ad_tag_size_string.split(', ')]
mediabuy_ad_tag_size_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_ad_tag_size_list]

mediabuy_market_string = 'AE, AL, AR, AT, AU, AZ, BA, BE, BG, BH, BR, BY, CA, CH, CL, CN, CO, CR, CZ, DE, DK, DO, EC, EE, EG, ES, FI, FR, UK, GB, GE, GR, GT, Gulf, HK, HR, HU, ID, IE, IL, IN, IQ, IT, JO, JP, KE, KR, KSA, KW, KZ, LB, LT, LV, ME, MK, MX, MY, NL, NO, NZ, OM, PA, PE, PH, PL, PR, PT, QA, RO, RS, RU, SA, SE, SG, SI, SK, TH, TR, TW, UA, UG, US, VN, XK, YE, UZ, ZA'
mediabuy_market_list = [item.strip() for item in mediabuy_market_string.split(', ')]
mediabuy_market_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_market_list]

mediabuy_subproduct_string = 'OIS, GM5, ADP, ADV, WAV, ALR, ALL, ANC, ALT, AMH, BAL, UBN, UBB, UBI, UBH, UBR, UBS, UBT, BAG, BIS, BIT, BLA, BVC, BOM, BOU, BRE, CSS, CAS, CAF, CAT, CEL, CES, CHD, CHA, CDP, CMB, CSH, CCV, COM, COJ, COD, COA, CRV, CRI, CPB, CSR, DCS, DTS, DIN, DOL, DDC, DMG, DMM, DSG, DGIC, DHC, DRE, EBL, EGM, EMT, ELL, ENT, ETM, EUK, ECD, ECG, ECM, EXE, EXG, EXM, EXR, EXP, FLU, FDP, FMB, FSP, FDS, FRE, FRO, GAL, GIT, GOL, GKS, GRN, GRS, GRF, GDA, HOO, HRS, HUB, ICT, ICD, IDG, ICE, INT, IWO, JWB, JEW, JFT, JFS, JUM, KAL, KAN, KAR, KBK, KBS, KSK, KBB, KFZ, KFB, KFP, KPB, KDS, KIT, KPF, KOK, LIG, LIM, LOC, LOO, LUB, LUC, MMS, MFB, MMM, MMX, MIC, MPB, MCC, MCB, MMSS, MVA, MAE, MAL, MGS, MAB, MAR, MAC, MFD, MSF, MFF, MET, MYW, MIL, MIR, MIS, MUB, MDG, NTC, NTD, NTM, NUD, NUT, OPT, OCE, ORA, ORO, ORG, ORM, ORC, ORP, PBT, PBD, PDG, PDA, PDD, PFP, PDW, PFF, PDP, PEM, PMB, PSH, CPK, POD, POT, PRM, RTB, RIP, RYC, RCS, ROY, SCH, SEA, SOC, SHE, SKT, SKP, SKG, SKH, SKW, SNI, SNP, SNN, SICC, SICPB, SNM, SNB, RNR, STB, STG, STM, SAG, SBS, STC, SUG, SUW, TAN, TAB, TEM, TOP, MU3, TCN, TRN, TRT, TUR, TWX, TCD, TCC, TWS, ULT, VPT, WAF, WHI, WHC, WHP, WHW, WIN, WDP, WMB, WSH, XBR, DTM, MMG, MPBG, MPBM, ORW, SKL, TMM, TTT'
mediabuy_subproduct_list = [item.strip() for item in mediabuy_subproduct_string.split(', ')]
mediabuy_subproduct_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_subproduct_list]

strategy_string = 'see, think, do, care, NA'
strategy_list = [item.strip() for item in strategy_string.split(', ')]
strategy_list = [''.join(c for c in s if c.isalnum()).lower() for s in strategy_list]

mediabuy_platform_string = 'dv360, dv36o, meta, snap, amz, pin, tt, bing, google-ads, twitter, reddit, beeswax, linkedin, tradedesk, direct, ot'
mediabuy_platform_list = [item.strip() for item in mediabuy_platform_string.split(', ')]
mediabuy_platform_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_platform_list]

mediabuy_partner_string = '365, 9HO, ABC, ACA, AMG, ADC, ADL, ADM, ADT, ADPA, ADPI, ASW, AFB, AKI, ALG, AMZ, ARA, ANG, ASO, ATM, AVT, ASM, BME, BAO, BAU, BAV, BW, BTO, BIN, BLS, BLM, BLX, BRT, BRS, BUZ, CBS, CBA, CSP, CMS, CF, CIP, CMT, CLL, CLM, COM, CON, CRIT, CTC, CW, DLM, DV, DIO, DIS, DSN, DMS, DD, DV360, DV36O, EAS, EBA, ECO, EDAD, EDAM, eqt, ESPN, ETT, FCB, FBI, FND, FIN, FIT, FLA, fsq, FOX, FOR, FOD, GLO, GOA, GAAG, GOM, GOG, GYA, GDN, GOP, GPMD, GRE, GRM, grd, GRN, GRPM, GG, HM, HOL, HOT, HULU, HYB, IHR, imdb, IMG, IMHO, INCA, IND, INS, INM, IG, JG, KCL, KOL, KVT, LAD, LR, LKI, LIS, LIV, LM, MH, MAN, MRV, MAG, MAD, MAT, MAX, MBA, MDF, MER, MIQ, MIM, MIX, MOB, MOM, MOC, MUM, MXP, MYC, M6, MTF, MYT, NABD, NASCAR, NAV, N2KL, NBC, NBS, NCM, NW10, NC, NXD, NFL, NA, NRA, OAT, OGR, OKR, OLX, OPE, Other, PAN, PED, PTD, PET, PHA, PIN, PLS, PLT, PDC, PKT, PLM, PPS, PRI, PRF, QIY, qut, RDT, RZN, RCD, RNM, ROKU, RST, RTI, SWK, SAP, SBS, SEG, SEV, SHD, SMR, SNA, SID, SMRG, SPF, TBMO, TGT, TST, TEA, TCC, TNE, TEN, DOD, TTD, THRL, TIK, TPT, TRE, TUB, TUR, TVE, TVN, TWT, TWI, und, UNS, UNI, UNR, VM, val, VDX, VZN, VEV, VI, VIZ, VNT, VIU, VK, VOOT, VPO, WCO, wmg, WYK, WWE, XAX, XIA, XPL, XPA, YAH, YAN, YZL, YOU, YT, Z5, WBD, YELP'
mediabuy_partner_list = [item.strip() for item in mediabuy_partner_string.split(', ')]
mediabuy_partner_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_partner_list]

mediabuy_objective_string = 'Awareness, Views, Engagement, Traffic, Leads, App Install, Conversions, App-Install, Awareness-Conversion'
mediabuy_objective_list = [item.strip() for item in mediabuy_objective_string.split(', ')]
mediabuy_objective_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_objective_list]

mediabuy_dimensions_string = 'na, 1280x720, 970x66, 40s, 120x90, 300x115, 19s, 1248x702, 640x360, 750x1174, 976x66, 336x280, 1608x850, 250x200, 320x50, 2560x2560, 402x596, 9s, 300x1050, 480x320, 1x1, 320x100, 700x90, 1024x66, 168x28, 468x60, 25s, 12s, 800x435, 7s, 1456x180, 5s, 728x90, 1280x970, 120x240, 35s, 300x250, 640x480, 600x90, 300x50, 1024x768, 180x150, 82x82, 20s, 960x600, 14s, 29s, 234x60, 985x60, 250x250, 250x50, 120x600, 15s, 970x250, 120x60, 970x90, 22s, 720x300, 300x125, 27s, 580x80, 64s, 18s, 630x920, 51s, 180x180, 21s, 414x125, 320x480, 36s, 300x600, 640x100, 210x185, 1290x680, 240x400, 300x100, 320x240, 600x380, 550x480, 43s, 1024x90, 120x20, 11s, 38s, 23s, 216x36, 0x0, 768x1024, 31s, 6s, 13s, 26s, 32s, 33s, 1280x100, 24s, 1712x136, 88x31, 30splus, 34s, 4s, 125x125, 10s, 1600x320, 30s, 16s, 8s, 52s, 1920x1080, 768x66, 375x200, 37s, 39s, 640x640, 600x500, 17s, 28s, 160x600, 1080x1920, 1200x1200, 1400x720, 1440x720, 1920x1440, 200x200, 60s, social-hybrid'
mediabuy_dimensions_list = [item.strip() for item in mediabuy_dimensions_string.split(', ')]
mediabuy_dimensions_list = [''.join(c for c in s if c.isalnum()).lower() for s in mediabuy_dimensions_list]

# COMMAND ----------

# dictionary
keys = ['language','buying_type','costing_model','mediabuy_campaign_type','audience_type','audience_desc','data_type','optimisation','placement_type','mediabuy_format','device','mediabuy_ad_tag_size','mediabuy_market','mediabuy_subproduct','strategy','mediabuy_platform','mediabuy_partner','mediabuy_objective','mediabuy_dimensions']

values = [language_list,buying_type_list,costing_model_list ,mediabuy_campaign_type_list ,audience_type_list ,audience_desc_list ,data_type_list ,optimisation_list ,placement_type_list ,mediabuy_format_list ,device_list ,mediabuy_ad_tag_size_list ,mediabuy_market_list ,mediabuy_subproduct_list ,strategy_list ,mediabuy_platform_list ,mediabuy_partner_list ,mediabuy_objective_list ,mediabuy_dimensions_list ]

mediaBuy_dict = {k: v for k, v in zip(keys, values)}

# mediaBuy_dict['audience_desc'].extend(['na','18-40','18-35','25-44','14-35','A13-54','f45plus'])
# mediaBuy_dict['audience_type'].extend(['na','int'])
# mediaBuy_dict['data_type'].extend(['na'])
# mediaBuy_dict['mediabuy_dimensions'].extend(['na','none','9x16','1080x1920','2x3','4x5'])
# mediaBuy_dict['device'].extend(['hybrid'])
# mediaBuy_dict['placement_type'].extend(['oth','bumper','snap-ads','Snap ads','feed/stories','feed/stories/reels','snap-ads-pbt'])
# mediaBuy_dict['mediabuy_format'].extend(['AR Lens'])

# COMMAND ----------

def preproc(x):
    return (''.join(c for c in x if c.isalnum())).lower()

preproc_udf = udf(preproc, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Cases

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Naming convention flag and calculated flag should be same

# COMMAND ----------

# MAGIC %md
# MAGIC #### Campaign

# COMMAND ----------

dim_campaign23 = dim_campaign23.withColumn(
    "calculated_naming_flag", when(
        (preproc_udf(col("campaign_type")).isin(campaign_dict['campaign_type'])) & \
        (preproc_udf(col('campaign_market')).isin(campaign_dict['campaign_market'])) & 
        (preproc_udf(col("campaign_subproduct")).isin(campaign_dict['campaign_subproduct'])) & 
        (preproc_udf(col("segment")).isin(campaign_dict['segment'])) & 
        (preproc_udf(col("region")).isin(campaign_dict['region'])) & 
        (preproc_udf(col("portfolio")).isin(campaign_dict['portfolio'])) & 
        (preproc_udf(col("business_channel")).isin(campaign_dict['business_channel'])) & 
        (preproc_udf(col("media_channel")).isin(campaign_dict['media_channel'])) & 
        (preproc_udf(col("media_objective")).isin(campaign_dict['media_objective'])) & 
        (preproc_udf(col("starting_month")).isin(campaign_dict['starting_month'])), 0).otherwise(1)
)

# COMMAND ----------

# All rows
dim_campaign23.select('gpd_campaign_id','country_id','marketregion_code','platform_desc','campaign_desc','campaign_start_date','campaign_end_date','calculated_naming_flag','naming_convention_flag').display()

# COMMAND ----------

# filter rows whose flags are not matching
campaign_mismatch = dim_campaign23.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
campaign_mismatch.display()

# COMMAND ----------

if campaign_mismatch.count() > 0:
    for each in campaign_dict.keys():
        not_eval = []
        x = campaign_mismatch.select(preproc_udf(col(each))).distinct().collect()
        unique_values = [preproc(r[0]) for r in x]

        for i in unique_values:
            if i not in campaign_dict[each]:
                not_eval.append(i)

        print(f"{each}: {not_eval}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Mediabuy
# MAGIC

# COMMAND ----------

dim_mediabuy23 = dim_mediabuy23.withColumn(
    "calculated_naming_flag", when(
        (preproc_udf(col("language")).isin(mediaBuy_dict['language'])) \
        & (preproc_udf(col('buying_type')).isin(mediaBuy_dict['buying_type'])) \
        & (preproc_udf(col("costing_model")).isin(mediaBuy_dict['costing_model'])) \
        & (preproc_udf(col("mediabuy_campaign_type")).isin(mediaBuy_dict['mediabuy_campaign_type'])) \
        & (preproc_udf(col("audience_type")).isin(mediaBuy_dict['audience_type'])) \
        & (preproc_udf(col("audience_desc")).isin(mediaBuy_dict['audience_desc'])) \
        & (preproc_udf(col("data_type")).isin(mediaBuy_dict['data_type'])) \
        & (preproc_udf(col("optimisation")).isin(mediaBuy_dict['optimisation'])) \
        & (preproc_udf(col("placement_type")).isin(mediaBuy_dict['placement_type'])) \
        & (preproc_udf(col("mediabuy_format")).isin(mediaBuy_dict['mediabuy_format'])) \
        & (preproc_udf(col("device")).isin(mediaBuy_dict['device'])) \
        & (preproc_udf(col("mediabuy_ad_tag_size")).isin(mediaBuy_dict['mediabuy_ad_tag_size'])) \
        & (preproc_udf(col("mediabuy_market")).isin(mediaBuy_dict['mediabuy_market'])) \
        & (preproc_udf(col("mediabuy_subproduct")).isin(mediaBuy_dict['mediabuy_subproduct'])) \
        & (preproc_udf(col("strategy")).isin(mediaBuy_dict['strategy'])) \
        & (preproc_udf(col("mediabuy_platform")).isin(mediaBuy_dict['mediabuy_platform'])) \
        & (preproc_udf(col("mediabuy_partner")).isin(mediaBuy_dict['mediabuy_partner'])) \
        & (preproc_udf(col("mediabuy_objective")).isin(mediaBuy_dict['mediabuy_objective'])) \
        & (preproc_udf(col("mediabuy_dimensions")).isin(mediaBuy_dict['mediabuy_dimensions'])), 0).otherwise(1)
)

# COMMAND ----------

# All rows
dim_mediabuy23.select('media_buy_id','country_id','marketregion_code','gpd_campaign_id','campaign_desc','media_buy_desc','platform_desc','campaign_start_date','campaign_end_date','calculated_naming_flag','naming_convention_flag').display()

# COMMAND ----------

# filter rows whose flags are not matching
mediabuy_mismatch = dim_mediabuy23.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
mediabuy_mismatch.display()

# COMMAND ----------

if mediabuy_mismatch.count() > 0:
    for each in mediaBuy_dict.keys():
        not_eval = []
        x = mediabuy_mismatch.select(col(each)).distinct().collect()
        unique_values = [preproc(r[0]) for r in x]

        for i in unique_values:
            if i not in mediaBuy_dict[each]:
                not_eval.append(i)

        print(f"{each}: {not_eval}")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Creative

# COMMAND ----------

dim_creative23 = dim_creative23.withColumn(
    "calculated_naming_flag", when(
    (preproc_udf(col("creative_variant")).isin(creative_dict['creative_variant'])) & \
    (preproc_udf(col('creative_type')).isin(creative_dict['creative_type'])) & \
    (preproc_udf(col("ad_tag_size")).isin(creative_dict['ad_tag_size'])) & \
    (preproc_udf(col("dimension")).isin(creative_dict['dimension'])) & \
    (preproc_udf(col("cta")).isin(creative_dict['cta'])) & \
    (preproc_udf(col("landing_page")).isin(creative_dict['landing_page'])) & \
    (preproc_udf(col("creative_market")).isin(creative_dict['creative_market'])) & \
    (preproc_udf(col("creative_subproduct")).isin(creative_dict['creative_subproduct'])) & \
    (preproc_udf(col("creative_language")).isin(creative_dict['creative_language'])) & \
    (preproc_udf(col("creative_platform")).isin(creative_dict['creative_platform'])) & \
    (preproc_udf(col("creative_partner")).isin(creative_dict['creative_partner'])) & \
    (preproc_udf(col("creative_campaign_type")).isin(creative_dict['creative_campaign_type'])) & \
    (preproc_udf(col("creative_audience_type")).isin(creative_dict['creative_audience_type'])) & \
    (preproc_udf(col("creative_audience_desc")).isin(creative_dict['creative_audience_desc'])), 0).otherwise(1)
)

# COMMAND ----------

dim_creative23.display()


# COMMAND ----------

# All rows
dim_creative23.select('creative_id','country_id','marketregion_code','gpd_campaign_id','campaign_desc','creative_desc','platform_desc','campaign_start_date','campaign_end_date','calculated_naming_flag','naming_convention_flag').display()

# COMMAND ----------

# filter rows whose flags are not matching
creative_mismatch = dim_creative23.filter(col("calculated_naming_flag")!=col("naming_convention_flag"))
creative_mismatch.display()

# COMMAND ----------

if creative_mismatch.count() > 0:
    for each in creative_dict.keys():
        not_eval = []
        x = creative_mismatch.select(col(each)).distinct().collect()
        unique_values = [preproc(r[0]) for r in x]

        for i in unique_values:
            if i not in creative_dict[each]:
                not_eval.append(i)
        
        print(f"{each}: {not_eval}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Count the numbers of following vs not following

# COMMAND ----------

dim_campaign23.groupby('marketregion_code')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_creative23.groupby('marketregion_code')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_mediabuy23.groupby('marketregion_code')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_campaign23.filter(col("naming_convention_flag")==1).display()

# COMMAND ----------

dim_creative23.filter(col("naming_convention_flag")==1).display()

# COMMAND ----------

from pyspark.sql.functions import col, sum as spark_sum
null_dim_campaign23 = dim_campaign23.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in dim_campaign23.columns])
null_dim_campaign23.display()

# COMMAND ----------

null_dim_creative23 = dim_creative23.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in dim_creative23.columns])
null_dim_creative23.display()

# COMMAND ----------

null_dim_mediabuy23 = dim_mediabuy23.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in dim_mediabuy23.columns])
null_dim_mediabuy23.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Count the numbers at region X platform level of following vs not following

# COMMAND ----------

df1 = fact_performance.select('gpd_campaign_id','channel_id').dropDuplicates()
dim_campaign23.join(df1, on = 'gpd_campaign_id',how = 'inner').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'inner').groupby('marketregion_code','platform_desc')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_creative23.display()

# COMMAND ----------

dim_creative23

# COMMAND ----------

df2 = fact_performance.select('creative_id','channel_id', 'country_id', 'gpd_campaign_id','campaign_desc', 'campaign_start_date', 'campaign_end_date').dropDuplicates()
df2 = df2.withColumnRenamed('campaign_start_date', 'campaign_start_date_')
df2 = df2.withColumnRenamed('campaign_end_date', 'campaign_end_date_')
df2 = df2.withColumnRenamed('country_id', 'country_id_')

dim_creative23.join(df2, on = 'creative_id',how = 'inner').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'inner').groupby('marketregion_code',
                                                            'country_id_',                                                               'platform_desc', 
                                                            'gpd_campaign_id','campaign_desc',
                                                            'campaign_start_date_', 
                                                            'campaign_end_date_')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

df2 = fact_performance.select('creative_id','channel_id', 'country_id', 'gpd_campaign_id','campaign_desc', 'campaign_start_date', 'campaign_end_date').dropDuplicates()
df2 = df2.withColumnRenamed('campaign_start_date', 'campaign_start_date_')
df2 = df2.withColumnRenamed('campaign_end_date', 'campaign_end_date_')
df2 = df2.withColumnRenamed('country_id', 'country_id_')

dim_creative23.join(df2, on = 'creative_id',how = 'inner').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'inner').groupby('marketregion_code',
                                                            'country_id_',                                                               'platform_desc', 
                                                            'gpd_campaign_id','campaign_desc',
                                                            'campaign_start_date_', 
                                                            'campaign_end_date_',
                                                            'naming_convension_flag')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

df2 = fact_performance.select('creative_id','channel_id').dropDuplicates()
dim_creative23.join(df2, on = 'creative_id',how = 'inner').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'inner').groupby('marketregion_code','platform_desc')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

df3 = fact_performance.select('media_buy_id','channel_id').dropDuplicates()
dim_mediabuy23.join(df3, on = 'media_buy_id',how = 'inner').join(dim_channel.select('channel_id','platform_desc'), on = 'channel_id',how = 'inner').groupby('marketregion_code','platform_desc')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("calculated_naming_flag")==0, True)).alias("following"),\
        count(when(col("calculated_naming_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  4. Count the numbers at region X country level of following vs not following

# COMMAND ----------

dim_campaign23.groupby('country_desc')\
    .agg(count('campaign_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_creative23.groupby('country_desc')\
    .agg(count('creative_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()

# COMMAND ----------

dim_mediabuy23.groupby('country_desc')\
    .agg(count('media_buy_desc').alias("total_count"),\
        count(when(col("naming_convention_flag")==0, True)).alias("following"),\
        count(when(col("naming_convention_flag")==1, True)).alias("not_following")).display()