from peewee import *

database = PostgresqlDatabase(None)

class UnknownField(object):
    def __init__(self, *_, **__): pass

class BaseModel(Model):
    class Meta:
        database = database


class RandomData(BaseModel):
    id = IntegerField()
    uid = CharField(null=False, unique = True)
    strain = CharField(null=True)
    cannabinoid_abbreviation = CharField()
    cannabinoid = CharField()
    terpene = CharField()
    medical_use = CharField()

    health_benefit = CharField()
    category = CharField()
    type = CharField()
    buzzword = CharField()
    brand = CharField()

    class Meta:
        table_name = 'randomdata'
