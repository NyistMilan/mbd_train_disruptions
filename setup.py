from setuptools import setup, find_packages

setup(
    name='mbd_train_disruptions',
    version='0.1',
    description='',
    url='',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
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
        'matplotlib',
        'seaborn',
        'scipy',
        'numpy',
    ]
)