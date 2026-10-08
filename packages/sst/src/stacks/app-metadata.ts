import { useBootstrap } from "../bootstrap.js";
import { useAWSClient } from "../credentials.js";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { Logger } from "../logger.js";
import { useProject } from "../project.js";
import { lazy } from "../util/lazy.js";

interface Metadata {
  mode: "deploy" | "dev";
}

async function metadata() {
  Logger.debug("Fetching app metadata");
  const bootstrap = await useBootstrap();
  const s3 = useAWSClient(S3Client);

  try {
    const result = await s3.send(
      new GetObjectCommand({
        Key: useS3Key(),
        Bucket: bootstrap.bucket,
      })
    );
    const body = await result.Body!.transformToString();
    return JSON.parse(body) as Metadata;
  } catch (ex) {
    Logger.debug("Fetching app metadata: not found");
  }
}

export async function saveAppMetadata(data: Metadata) {
  Logger.debug("Saving app metadata");
  const bootstrap = await useBootstrap();
  const s3 = useAWSClient(S3Client);

  try {
    await s3.send(
      new PutObjectCommand({
        Key: useS3Key(),
        Bucket: bootstrap.bucket,
        Body: JSON.stringify(data),
      })
    );
  } catch (ex) {
    Logger.debug("Saving app metadata: not found");
  }
}

export async function clearAppMetadata() {
  Logger.debug("Clearing app metadata");
  const bootstrap = await useBootstrap();
  const s3 = useAWSClient(S3Client);

  await s3.send(
    new DeleteObjectCommand({
      Key: useS3Key(),
      Bucket: bootstrap.bucket,
    })
  );
}

function useS3Key() {
  const project = useProject();
  return `appMetadata/app.${project.config.name}/stage.${project.config.stage}.json`;
}

export const useAppMetadata = lazy(async () => {
  const data = await metadata();
  return data;
});
