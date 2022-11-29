from faker import Faker
from typing import Dict

faker = Faker()


def _generate_fake_value(dt: str):
   generator = {
       "int": faker.unique.pyint(),
       "datetime": faker.date_time().strftime("%m/%d/%YT%H:%M:%S"),
       "md5": faker.md5(),
       "str": faker.text(),
       "uuid": faker.uuid4(),
   } 

   return generator[dt]


def generate_dummy_data(schema: Dict, count: int):
    data = []

    for idx in range(count):
        obj = {}

        for key, dt in schema.items():
            obj[key] = _generate_fake_value(dt)

        data.append(obj)

    return data