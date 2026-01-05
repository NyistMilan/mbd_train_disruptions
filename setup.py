from setuptools import setup

setup(
    name='mbd_train_disruptions',
    version='0.1',
    description='',
    url='',
    packages=['mbd_train_disruptions'],
    package_dir={'mbd_train_disruptions': 'src'},
    license='',
    author='Milan Konor Nyist',
    python_requires='',
    install_requires=[
        'requests',
        'beautifulsoup4',
        'pandas',
        'xarray',
        'pyarrow',
        'fastparquet',
        'aiohttp',
        'asyncio', 
    ]
)