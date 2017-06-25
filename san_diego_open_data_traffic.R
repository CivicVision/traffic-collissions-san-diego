library('dplyr')
library(jsonlite)

sd_adddresses = read.csv("san_diego_addresses.csv")
sd_accidents = read.csv("http://seshat.datasd.org/pd/pd_collisions_datasd.csv",stringsAsFactors=F)

police_beats = read.csv("http://seshat.datasd.org/pd/pd_beat_neighborhoods_datasd.csv", stringsAsFactors=F,colClasses="character")

write(toJSON(police_beats), "police_beats.json")
sd_accidents$date = as.Date(sd_accidents$date_time)
sd_accidents$year = format(sd_accidents$date, "%Y")

write.csv(sd_accidents, "accidents.csv")
sd_accidents.killed = sd_accidents %>% filter(killed > 0)
sd_accidents.injured = sd_accidents %>% filter(injured > 0)

sd_accidents.2017 = sd_accidents %>% filter(date > "2016-12-31")
sd_accidents.2016 = sd_accidents %>% filter(year == 2016)

sd_accidents.2017.killed = sd_accidents.2017 %>% filter(killed > 0)
write.csv(sd_accidents.2017.killed, "accidents_killed_2017.csv")

sd_accidents.killed %>% group_by(year) %>% summarise(count = sum(killed))
sd_accidents.injured %>% group_by(year) %>% summarise(count = sum(injured))

sd_accidents.killed_injured = sd_accidents %>% filter(killed > 0 | injured > 0)
write.csv(sd_accidents.killed_injured, "accidents_killed_injured.csv")

sd_accidents.killed_injured.year = sd_accidents %>% group_by(year) %>% summarise(injured=sum(injured), killed=sum(killed))
write.csv(sd_accidents.killed_injured.year, "accidents_killed_injured_by_year.csv")

toJSON(sd_accidents.killed_injured.year)

sd_accidents.killed_injured[1,]

injured_killed_by_police_beat = sd_accidents %>% group_by(police_beat) %>% summarise(injured=sum(injured), killed=sum(killed)) 

sd_accidents.killed_injured.year.beat = sd_accidents %>% group_by(police_beat,year) %>% summarise(injured=sum(injured), killed=sum(killed), accidents=n()) %>% left_join(police_beats, by=c("police_beat" = "Beat"))
injured_killed_by_police_beat.neighborhoods = injured_killed_by_police_beat %>% left_join(police_beats, by=c("police_beat" = "Beat"))

write.csv(injured_killed_by_police_beat.neighborhoods, "injured_killed_by_police_beat.csv")
write.csv(sd_accidents.killed_injured.year.beat, "injured_killed_by_year_and_police_beat.csv")

## Just a test
sd_accidents %>% group_by(street_name) %>% summarise(count=n()) %>% arrange(-count)

sd_accidents %>% group_by(violation_type) %>% summarise(count=n()) %>% arrange(-count)

sd_accidents.university = sd_accidents %>% filter(street_name == "UNIVERSITY")

university_street_no = as.data.frame(sd_accidents.university$street_no)
names(university_street_no) = c("street_no")
university_street_no.quantile = university_street_no %>% mutate(quintile = ntile(street_no, 10))


sd_accidents.university.q = merge(sd_accidents.university,university_street_no.quantile,all.x = T)
