<?php
/**
 * @author Nicolas CARPi <nico-git@deltablot.email>
 * @copyright 2022 Nicolas CARPi
 * @see https://www.elabftw.net Official website
 * @license AGPL-3.0
 * @package elabftw
 */
declare(strict_types=1);

namespace Elabftw\Storage;

use Aws\S3\S3Client;
use Aws\S3\S3ClientInterface;
use Elabftw\Models\Config;
use League\Flysystem\AwsS3V3\AwsS3V3Adapter;
use League\Flysystem\FilesystemAdapter;

/**
 * Provide a League\Filesystem adapter for S3 buckets file uploads
 * Patched for GCS Uniform Bucket-Level Access (no ACLs/visibility)
 */
class S3 extends AbstractStorage
{
    protected const string S3_VERSION = '2006-03-01';
    protected const int PART_SIZE = 104857600;

    public function __construct(
        protected readonly Config $config,
    ) {}

    public function getPath(string $relativePath = ''): string
    {
        return ($this->config->path_prefix ?? '') . ($relativePath !== '' ? '/' . $relativePath : '');
    }

    public function getAbsoluteUri(string $path): string
    {
        return 's3://' . ($this->config->s3_bucket_name ?? '') . '/' . $this->getPath($path);

    }

    protected function getAdapter(): FilesystemAdapter
    {
        $client = $this->getClient();
        $client->registerStreamWrapper();

        return new AwsS3V3Adapter(
            $client,
            $this->config->s3_bucket_name ?? '',
            $this->config->s3_path_prefix ?? '',
            options: ['part_size' => self::PART_SIZE],
        );
    }

    protected function getClient(): S3ClientInterface
    {
        return new S3Client([
            'version'     => self::S3_VERSION,
            'region'      => $this->config->s3_region ?? 'asia-southeast1',
            'endpoint'    => $this->config->s3_endpoint ?? 'https://storage.googleapis.com',
            'credentials' => [
                'key'    => $this->config->s3_access_key ?? '',
                'secret' => $this->config->s3_secret_key ?? '',
            ],
            'use_aws_shared_config_files' => false,
            'use_path_style_endpoint'     => (bool) ($this->config->s3_use_path_style_endpoint ?? false),
            'http' => [
                'verify' => (bool) ($this->config->s3_verify_cert ?? true),
            ],
        ]);
    }
}