from setuptools import setup, find_packages
import os

# Read the contents of your README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='htan_validation',
    version='0.0.1',
    author="Yamina Katariya, Dar'ya Pozhidayeva",
    author_email='ykatariy@systemsbiology.org, dpozhida@systemsbiology.org',
    description='A short description of your package',
    long_description=long_description,
    long_description_content_type='text/markdown',
    # url='https://github.com/ncihtan/htan2-data-release-pipeline',
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
    install_requires=[
        # List your package dependencies here, e.g., 'requests>=2.20.0'
    ],
    # entry_points={
    #     'console_scripts': [
    #         'your_script_name = your_package_name.module:main_function',
    #     ],  
    # },
)