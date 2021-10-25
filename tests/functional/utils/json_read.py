import json
import sys
import os
from pathlib import Path
c = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


async def load_json(index_name: str, id: str):
    with open(f'{c}/testdata/{index_name}.json', 'r') as jfile:
        movies_list = [
                                json.dumps(
                                    {
                                        'index': {
                                            '_index': f'{index_name}',
                                            '_id': f'{id}'
                                        }
                                    }
                                ),
                                f'{json.load(jfile)}'
                            ]
    return '\n'.join(movies_list) + '\n'