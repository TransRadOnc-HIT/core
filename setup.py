from setuptools import setup, find_packages

setup(name='core',
      version='1.0',
      description='Repository with several utilities',
      url='https://github.com/TransRadOnc-HIT/core.git',
      python_requires='>=3.5',
      author='Francesco Sforazzini',
      author_email='f.sforazzini@dkfz.de',
      license='Apache 2.0',
      zip_safe=False,
      install_requires=[
      'nipype',
      'pydicom==1.2.2'],
      dependency_links=['git+https://github.com/TransRadOnc-HIT/nipype.git@c453eac5d7efdd4e19a9bcc8a7f3d800026cc125#egg=nipype-9876543210'],
      packages=find_packages(exclude=['*.tests', '*.tests.*', 'tests.*', 'tests']),
      classifiers=[
          'Intended Audience :: Science/Research',
          'Programming Language :: Python',
          'Topic :: Scientific/Engineering',
          'Operating System :: Unix'
      ]
      )
