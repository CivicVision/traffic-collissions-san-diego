library('dplyr')

person = read.csv('FARS2015NationalCSV/person.csv')

data = read.csv('FARS2015NationalCSV/ACC_AUX.CSV')

san_diego_county = tbl_df(data) %>% filter(STATE == 6, COUNTY == 73)
person.san_diego_county = tbl_df(person) %>% filter(STATE == 6, COUNTY == 73)

# Fatalities
person.san_diego_county.fatalities = person.san_diego_county %>% filter(INJ_SEV == 4)

summary(person.san_diego_county.fatalities)

# Pedestrian Fatalities
person.san_diego_county.fatalities.pedestrian = person.san_diego_county.fatalities %>% filter(PER_TYP == 5)
person.san_diego_county.fatalities.pedestrian.sex = person.san_diego_county.fatalities.pedestrian %>% group_by(SEX) %>% summarize(count = n())
person.san_diego_county.fatalities.pedestrian.age = person.san_diego_county.fatalities.pedestrian %>% group_by(AGE) %>% summarize(count = n())
person.san_diego_county.fatalities.pedestrian %>% group_by(SEX,AGE) %>% summarize(count = n())

# Merge with accidents data for lat/long
accidents = read.csv('FARS2015NationalCSV/accident.csv')
accidents.san_diego_county = tbl_df(accidents) %>% filter(STATE == 6, COUNTY == 73)

write.csv(accidents.san_diego_county, "accidents_sd_county.csv")


accidents.person.fatalities = person.san_diego_county.fatalities %>% left_join(accidents.san_diego_county, by="ST_CASE")
write.csv(accidents.person.fatalities, "accidents_persons_sdc.csv")
