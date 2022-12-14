from distutils.core import setup

with open('LICENSE') as handle:
    license = handle.read()

with open('README.md') as handle:
    long_description = handle.read()

setup(
    name='blobmetafs',
    version='0.1dev',
    packages=['bmfs', 'bmkit'],
    license=license,
    long_description=long_description,
    long_description_content_type='text/markdown',
    entry_points = {
        'console_scripts': ['bmkit.initdata=bmkit.main:initdata',
                            'bmkit.shell=bmkit.main:shell',
                            'blobmetafs=bmfs.main:main'],
    },
    package_data = {
        'bmkit': ['sample_data/*']
    },
    install_requires = [
        'pyfuse3',
        'peewee',
    ],
    extras_require = {
        'testing': [
            'pytest',
        ],
    },
)
