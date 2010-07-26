from setuptools import setup, find_packages
import os

DESCRIPTION = "A ElasticSearch backend standing outside django (>= 1.2)"

LONG_DESCRIPTION = None
try:
    LONG_DESCRIPTION = open('README.rst').read()
except:
    pass

def get_version(version_tuple):
    version = '%s.%s' % (version_tuple[0], version_tuple[1])
    if version_tuple[2]:
        version = '%s.%s' % (version, version_tuple[2])
    return version

init = os.path.join(os.path.dirname(__file__), 'django_elasticsearch', '__init__.py')
print init
version_line = filter(lambda l: l.startswith('VERSION'), open(init))[0]
VERSION = get_version(eval(version_line.split('=')[-1]))
print VERSION

CLASSIFIERS = [
    'Development Status :: 4 - Beta',
    'Intended Audience :: Developers',
    'License :: OSI Approved :: MIT License',
    'Operating System :: OS Independent',
    'Programming Language :: Python',
    'Topic :: Database',
    'Topic :: Software Development :: Libraries :: Python Modules',
]

setup(name='django_elasticsearch',
      version=VERSION,
      packages=find_packages(),
      author='Alberto Paro',
      author_email='alberto@{nospam}ingparo.it',
      url='http://github.com/aparo/django-elasticsearch',
      license='MIT',
      include_package_data=True,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      platforms=['any'],
      classifiers=CLASSIFIERS,
      install_requires=['pyes'],
      test_suite='tests',
)
