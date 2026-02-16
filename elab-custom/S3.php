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
        return $this->config->get('path_prefix') . ($relativePath !== '' ? '/' . $relativePath : '');
    }

    public function getAbsoluteUri(string $path): string
    {
        return 's3://' . $this->config->get('bucket_name') . '/' . $this->getPath($path);
    }

    protected function getAdapter(): FilesystemAdapter
    {
        $client = $this->getClient();
        $client->registerStreamWrapper();

        return new AwsS3V3Adapter(
            $client,
            $this->config->get('bucket_name'),
            $this->config->get('path_prefix'),
            options: ['part_size' => self::PART_SIZE],
        );
    }

    protected function getClient(): S3ClientInterface
    {
        return new S3Client([
            'version'     => self::S3_VERSION,
            'region'      => $this->config->get('aws_region') ?? 'asia-southeast1',
            'endpoint'    => $this->config->get('aws_endpoint') ?? 'https://storage.googleapis.com',
            'credentials' => [
                'key'    => $this->config->get('aws_access_key') ?? '',
                'secret' => $this->config->get('aws_secret_key') ?? '',
            ],
            'use_aws_shared_config_files' => false,
            'use_path_style_endpoint'     => (bool) $this->config->get('aws_use_path_style_endpoint'),
            'http' => [
                'verify' => (bool) $this->config->get('aws_verify_cert', true),
            ],
        ]);
    }
}