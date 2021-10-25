import json
import os

c = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


async def load_json(file: str, index_name: str, id: str):
    with open(f'{c}/testdata/{file}', 'r') as jfile:
        movies_list = [
                                json.dumps(
                                    {
                                        'index': {
                                            '_index': f'{index_name}',
                                            '_id': f'{id}'
                                        }
                                    }
                                ),
                                json.dumps(json.load(jfile))
                            ]
    return '\n'.join(movies_list) + '\n'
