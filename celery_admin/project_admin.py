import sys

sys.path.append('../')
from etl_tasks.etl_target.db_postgre import RandomData


class ETLTask:
    random_data = RandomData()

    def init_db(self, _config):
        self.random_data._meta.database.init(
            _config.settings.target_database.dbname,
            host= _config.settings.target_database.host,
            port= _config.settings.target_database.port,
            user= _config.settings.target_database.user,
            password= _config.settings.target_database.password
        )
        self.random_data.create_table()

    def upsert_data(self, ins_dict):
        try:
            rowid = (RandomData
            .insert(
                    id = ins_dict.get('id')
                    , uid = ins_dict.get('uid')
                    , strain = ins_dict.get('strain')
                    , cannabinoid_abbreviation = ins_dict.get('cannabinoid_abbreviation')
                    , cannabinoid = ins_dict.get('cannabinoid')
                    , terpene = ins_dict.get('terpene')
                    , medical_use = ins_dict.get('medical_use')
                    , health_benefit = ins_dict.get('health_benefit')
                    , category = ins_dict.get('category')
                    , type = ins_dict.get('type')
                    , buzzword = ins_dict.get('buzzword')
                    , brand = ins_dict.get('brand')
                    )
            .on_conflict(
                conflict_target=[RandomData.uid],  # Which constraint?
                update={
                    RandomData.id: ins_dict.get('id')
                    , RandomData.strain: ins_dict.get('strain')
                    , RandomData.cannabinoid_abbreviation: ins_dict.get('cannabinoid_abbreviation')
                    , RandomData.cannabinoid : ins_dict.get('cannabinoid')
                    , RandomData.terpene: ins_dict.get('terpene')
                    , RandomData.medical_use: ins_dict.get('medical_use')
                    , RandomData.health_benefit: ins_dict.get('health_benefit')
                    , RandomData.category: ins_dict.get('category')
                    , RandomData.type: ins_dict.get('type')
                    , RandomData.buzzword: ins_dict.get('buzzword')
                    , RandomData.brand: ins_dict.get('brand')
                })
            .execute())

            return (True)
        except Exception as e:
            func_name = sys._getframe().f_code.co_name
            print(f'{func_name} ERROR: {e}')
            return (False)
