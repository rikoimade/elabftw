
FROM elabftw/elabimg:5.3.11
# S3.php copied to the same directory
COPY S3.php /elabftw/src/Storage/S3.php
# nginx:nginx is problematic, changing to 101:101)
RUN chown -R 101:101 /elabftw/src/Storage/S3.php
