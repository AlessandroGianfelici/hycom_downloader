from setuptools import setup, find_packages

setup(name='hycom_downloader',
      version='0.1.1',
      description='Script to download simulation data from HYCOM.org',
      url='https://github.com/AlessandroGianfelici/hycom_downloader',
      author='Alessandro Gianfelici',
      author_email='alessandro.gianfelici@hotmail.com',
      license='MIT',
      packages=find_packages(),
      include_package_data=True,
      install_requires=[
          'pandas', 
          'beautifulsoup4',
          'requests',
          'xarray',
          'toolz'
      ],
      zip_safe=False)