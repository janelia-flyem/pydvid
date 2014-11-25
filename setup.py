from setuptools import setup

packages=['pydvid', 
          'pydvid.general',
          'pydvid.keyvalue',
          'pydvid.labelgraph',
          'pydvid.voxels',
          'pydvid.gui' ]

package_data={'pydvid': ['dvidschemas/json/*/*.schema.json']}

setup(name='pydvid',
      version='0.1',
      description='Python Access to DVID HTTP REST API',
      url='https://github.com/janelia-flyem/pydvid',
      packages=packages,
      package_data=package_data,
      setup_requires=['jsonschema>=1.0']
     )
