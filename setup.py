from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession


__version__ = '0.0.1'


it = parse_requirements('requirements.txt', session=PipSession())

setup(
    name='fused',
    author='vaultah',
    license='MIT',
    url='https://github.com/vaultah/fused',
    version=__version__,
    install_requires=[str(ir.req) for ir in it],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    packages=find_packages(),
)