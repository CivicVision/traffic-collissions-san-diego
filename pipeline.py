import agate
import agateremote
import proof
import boto3


def load_data(data):
    data['table'] = agate.Table.from_url('http://seshat.datasd.org/pd/pd_collisions_datasd.csv')

def load_police_beats(data):
    data['police_beats'] = agate.Table.from_url("http://seshat.datasd.org/pd/pd_beat_neighborhoods_datasd.csv")

def add_year_column(data):
    data['table'] = data['table'].compute([
        ('year', agate.Formula(agate.Text(), lambda r: r['date_time'].strftime("%Y"))),
        ])

def add_full_hour_date(data):
    data['table'] = data['table'].compute([
        ('date_hour', agate.Formula(agate.Text(), lambda r: r['date_time'].strftime("%Y-%m-%d %H:00:00"))),
        ])

def sum_counts_by_full_hour(data):
    data['full_hour']= data['table'].group_by('date_hour').aggregate([
        ('killed', agate.Sum('killed')),
        ('injured', agate.Sum('injured')),
        ('accidents', agate.Count())
    ])

def year_sum_counts(data):
    data['groupped_year']= data['table'].group_by('year').aggregate([
        ('killed', agate.Sum('killed')),
        ('injured', agate.Sum('injured')),
        ('accidents', agate.Count())
    ])

def year_police_beat_sum_counts(data):
    data['year_police_beat']= data['table'].group_by('year').group_by('police_beat').aggregate([
        ('killed', agate.Sum('killed')),
        ('injured', agate.Sum('injured')),
        ('accidents', agate.Count())
    ])

def upload_killed_injured_year(data):
    data['groupped_year'].to_csv('akiy.csv')
    session = boto3.session.Session(profile_name='zoning')
    s3 = session.resource('s3')
    s3.Bucket('traffic-sd').put_object(Key='accicents_killed_injured_b_year.csv', Body=open('akiy.csv', 'rb'))

def upload_killed_injured_year_police_beat(data):
    data['year_police_beat'].to_csv('akiypb.csv')
    session = boto3.session.Session(profile_name='zoning')
    s3 = session.resource('s3')
    s3.Bucket('traffic-sd').put_object(Key='accicents_killed_injured_b_year_police_beat.csv', Body=open('akiypb.csv', 'rb'))

def upload_accidents(data):
    data['table'].to_csv('accidents.csv')
    session = boto3.session.Session(profile_name='zoning')
    s3 = session.resource('s3')
    s3.Bucket('traffic-sd').put_object(Key='accicents.csv', Body=open('accidents.csv', 'rb'))

def upload_full_hour(data):
    data['full_hour'].to_csv('full_hour.csv')
    session = boto3.session.Session(profile_name='zoning')
    s3 = session.resource('s3')
    s3.Bucket('traffic-sd').put_object(Key='full_hour_accidents.csv', Body=open('full_hour.csv', 'rb'))

def print_year_pb_data(data):
    data['year_police_beat'].print_table()

def print_year_data(data):
    data['groupped_year'].print_table()

def print_full_hour_data(data):
    data['full_hour'].print_table()

def print_data(data):
    data['table'].print_table(max_columns=None)

data_loaded = proof.Analysis(load_data)
year_data = data_loaded.then(add_year_column)
groupped_data = year_data.then(year_sum_counts)
groupped_data.then(upload_killed_injured_year)

year_police_beat_data = year_data.then(year_police_beat_sum_counts)
year_police_beat_data.then(upload_killed_injured_year_police_beat)

data_loaded.then(upload_accidents)

hour_data = data_loaded.then(add_full_hour_date)
full_hour_data = hour_data.then(sum_counts_by_full_hour)
full_hour_data.then(upload_full_hour)

data_loaded.run()
