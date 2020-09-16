using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.IO;

using Amazon.S3;
using Amazon.S3.IO;
using Amazon.S3.Transfer;
using Amazon.S3.Model;

/*
 * Create an application which recursively traverses the files of a directory and makes a backup to 
 * the cloud. You will use the cloud storage APIS to do this
 * 
 * This will make a backup to the cloud of the current directory and sub-directories to
 * either Azure of AWS. The directory structure of the files should be respected
 * 
 * Given the varieties of possibilities a key part of this will be clear instructions given to the
 * grader on how to get the application to work.
 * Assume you are releasing an open-source project that can be used by the grader.
 * 
 * You will need to be specific about where keys and such need to be placed.
 * As bandwidth is expensive your program should minimize the amount of data that
 * needs to be moved. For instance, if a directory is backed up a second time only
 * modified files should be updated.
 * 
 * https://docs.aws.amazon.com/AmazonS3/latest/dev/HLuploadFileDotNet.html
 */



namespace Amazon
{
    class Program
    {
        //global variables
        public static RegionEndpoint globalRegionPoint;
        public static Amazon.Runtime.BasicAWSCredentials globalCredentials;
        public static string globalBucketName;
        public static string globalAccessID;
        public static string globalSecretID;
        public static string globalStartFilePath;






        /* Main
         * The starting point of the program. Takes in user's arguments, assigns them to proper variables, and initiates
         * recursive upload method
         */
        public static void Main(string[] args)
        {
            try
            {
                //Set-up for variables
                setUpVariables(args);
                string filePath = globalStartFilePath;
                string bucketName = globalBucketName;
                var awsCredentials = new Amazon.Runtime.BasicAWSCredentials(globalAccessID, globalSecretID);
                var s3Client = new AmazonS3Client(awsCredentials, globalRegionPoint);
                //sets global variables
                globalCredentials = awsCredentials;


                //helps initialize recursion
                helperRecursive(s3Client, filePath, bucketName + extractKeyName(filePath));
            }
            catch (Exception)
            {
                Console.WriteLine("Please try again");
            }
        }






        /* helperRecursive()
         * helper method for recursive method
         */
        private static async Task helperRecursive(AmazonS3Client s3Client, string filePath, string bucketName)
        {
            Task t = Task.Run(() => recursiveUpload(s3Client, filePath, bucketName));
            t.Wait();
        }






        /* setUpVariables()
         * Takes list of user's arguments and attempts to initialize AWS variables 
         * them properly. Return a bool indiciating whether it was successful or not
         */
        private static void setUpVariables(string[] args)
        {
            //minimum number of arguments required
            if (args.Length < 5)
            {
                Console.WriteLine("Not enough parameters passed in");
                throw new Exception();
            }
            else
            {
                try
                {
                    globalAccessID = args[0];
                    globalSecretID = args[1];
                    globalBucketName = args[2] + "/";
                    globalRegionPoint = identifyRegionPoint(args[3]);
                    globalStartFilePath = determineStartFilePath(args);
                }
                catch (Exception)
                {
                    Console.WriteLine("Bad parameters passed in, was not able to initalize AWS variables");
                    throw new Exception();
                }
            }
        }






        /* determineStartFilePath()
         * A helper method for seUpVariables()
         * Takes the list of user arguments and builds a string seperated by spaces
         */
        private static string determineStartFilePath(string[] args)
        {
            string startFilePath = "";
            for (int i = 4; i < args.Length; i++)
            {
                startFilePath = startFilePath + args[i] + " ";
            }

            //remove last uneccessary space
            startFilePath = startFilePath.Substring(0, startFilePath.Length - 1);

            return startFilePath;
        }






        /* identifyRegionPoint()
         * A helper method for setUpVariables()
         * Based on user's input returns the appropriate RegionEndpoint, otherwise throws an exception
         * for setUpVariables to handle
         */
        private static RegionEndpoint identifyRegionPoint(string region)
        {
            //converts to lower case to handle more user cases
            region = region.ToLower();
            if (region.Equals("apeast1"))
            {
                return RegionEndpoint.APEast1;
            }
            else if (region.Equals("apnortheast1"))
            {
                return RegionEndpoint.APNortheast1;
            }
            else if (region.Equals("apnortheast2"))
            {
                return RegionEndpoint.APNortheast2;
            }
            else if (region.Equals("apnortheast3"))
            {
                return RegionEndpoint.APNortheast3;
            }
            else if (region.Equals("apnorth1"))
            {
                return RegionEndpoint.APSouth1;
            }
            else if (region.Equals("apsoutheast1"))
            {
                return RegionEndpoint.APSoutheast1;
            }
            else if (region.Equals("apsoutheast2"))
            {
                return RegionEndpoint.APSoutheast2;
            }
            else if (region.Equals("cacentral1"))
            {
                return RegionEndpoint.CACentral1;
            }
            else if (region.Equals("cnnorth1"))
            {
                return RegionEndpoint.CNNorth1;
            }
            else if (region.Equals("cnnorthWest1"))
            {
                return RegionEndpoint.CNNorthWest1;
            }
            else if (region.Equals("eucentral1"))
            {
                return RegionEndpoint.EUCentral1;
            }
            else if (region.Equals("eunorth1"))
            {
                return RegionEndpoint.EUNorth1;
            }
            else if (region.Equals("euwest1"))
            {
                return RegionEndpoint.EUWest1;
            }
            else if (region.Equals("euwest2"))
            {
                return RegionEndpoint.EUWest2;
            }
            else if (region.Equals("euwest3"))
            {
                return RegionEndpoint.EUWest3;
            }
            else if (region.Equals("mesouth1"))
            {
                return RegionEndpoint.MESouth1;
            }
            else if (region.Equals("saeast1"))
            {
                return RegionEndpoint.SAEast1;
            }
            else if (region.Equals("useast1"))
            {
                return RegionEndpoint.USEast1;
            }
            else if (region.Equals("useast2"))
            {
                return RegionEndpoint.USEast2;
            }
            else if (region.Equals("usgovcloudeast1"))
            {
                return RegionEndpoint.USGovCloudEast1;
            }
            else if (region.Equals("usgovcloudwest1"))
            {
                return RegionEndpoint.USGovCloudWest1;
            }
            else if (region.Equals("uswest1"))
            {
                return RegionEndpoint.USWest1;
            }
            else if (region.Equals("uswest2"))
            {
                return RegionEndpoint.USWest2;
            }
            else
            {
                throw new Exception();
            }
        }






        /* recursiveUpload()
         * recursively travels through provided directory and all subdirectories and uploads contents onto desired bucket
         * Does not reupload file that is already in the bucket UNLESS it has been modified since. Retains original structure
         * of directories
         * 
         * Note: Due to structure of S3, Folders only exist if objects exist in them. Therefore, if file encountered is a folder
         * it will re-enter this function recursively after calling uploadFileAsync()
         */
        private static async Task recursiveUpload(AmazonS3Client s3Client, string filePath, string bucketName)
        {
            //create list of files in the current directory
            try
            {
                string[] listOfFiles = Directory.GetFileSystemEntries(filePath, "*", SearchOption.TopDirectoryOnly);


                //traverses through all the finals in current directory of bucket
                for (int i = 0; i < listOfFiles.Length; i++)
                {


                    //checks if file exists in bucket
                    if (doesFileExist(s3Client, listOfFiles[i], bucketName))
                    {

                        //if file needs to be updated in bucket
                        if (checkMetaData(s3Client, listOfFiles[i], bucketName)) //pass in filePath as well!!!!!!
                        {
                            await uploadFileAsync(s3Client, listOfFiles[i], bucketName);
                        }
                    }
                    else
                    {
                        //uploads file since it doesn't exist in bucket
                        await uploadFileAsync(s3Client, listOfFiles[i], bucketName);
                    }
                }
            }
            catch (Exception)
            {
                Console.WriteLine("An exception occured");
                Console.WriteLine("The file: " + filePath + " was not able to backed up");
            }
        }






        /* createTempFile
         * Temporarily creates a file in provided empty directory to use as a temporary file in bucket
         * This is done so that empty directories would be visible in bucket
         */
        private static async Task createTempFile(AmazonS3Client s3Client, string filePath, string bucketName)
        {
            //creates a file with random name in the current directory of user
            string tempFile = Path.GetRandomFileName();
            FileStream fs = File.Create(filePath + "\\" + tempFile);
            fs.Close();


            //uploads temporary file up to the cloud
            AmazonS3Client tempClient = new AmazonS3Client(globalCredentials, globalRegionPoint);
            Task t = uploadFileAsync(tempClient, filePath + "\\" + tempFile, bucketName);
            await t;
        }






        /* checkMetaData
         * Checks when file was last updated. If the file on user's computer has been updated since being uploaded
         * Then returns true. Otherwise returns false
         */
        private static bool checkMetaData(AmazonS3Client s3Client, string filePath, string bucketName)
        {
            ListObjectsRequest listRequest = new ListObjectsRequest();
            //listRequest.BucketName = bucketName;
            listRequest.BucketName = globalBucketName;
            ListObjectsResponse listResponse;


            //formats key properly
            string key = bucketName + "/" + extractKeyName(filePath);
            key = key.Replace(globalBucketName, "");

            do
            {
                listResponse = s3Client.ListObjects(listRequest);
                foreach (S3Object obj in listResponse.S3Objects)
                {
                    if (obj.Key.Equals(key))
                    {
                        var lastModifiedUserFile = File.GetLastWriteTime(filePath);

                        //compares write times and returns boolean
                        if (lastModifiedUserFile.CompareTo(obj.LastModified) > 0)
                        {
                            return true;
                        }
                        else
                        {
                            return false;
                        }
                    }
                }
            } while (listResponse.IsTruncated);


            //should not reach this point under any circumstance
            return false;
        }






        /* isDirectory
         * Returns true if provided file path is a directory, and false if it is a file
         */
        private static bool isDirectory(string filePath)
        {
            //it's a directory
            if ((File.GetAttributes(filePath) & FileAttributes.Directory) == FileAttributes.Directory)
            {
                return true;
            }
            //it's a file
            else
            {
                return false;
            }
        }






        /* doesFileExist
         * Checks to see if the file with the same name as the one provided by the file path
         * exists inside the bucket. Returns true if exists, and false if it doesn't
         */
        private static bool doesFileExist(AmazonS3Client s3Client, string filePath, string bucketName)
        {
            string key = extractKeyName(filePath);
            S3FileInfo fileInfo = new S3FileInfo(s3Client, bucketName, key);

            if (fileInfo.Exists)
            {
                return true;
            }
            else
            {
                return false;
            }
        }






        /* extractKeyName
         * Extracts the would-be key name from the filePath string, returns that string
         */
        private static string extractKeyName(string filePath)
        {
            char delimiter = '\\';
            string[] words = filePath.Split(delimiter);
            return words[words.Length - 1];
        }






        /* uploadFileAsync
         * uploads a file to the bucket 
         */
        private static async Task uploadFileAsync(AmazonS3Client s3Client, string filePath, string bucketName)
        {
            try
            {
                //checks to see if the file that needs to uploaded is a file or a directory
                if (isDirectory(filePath))
                {
                    string key = extractKeyName(filePath);
                    string subBucket = bucketName + "/" + key;
                    await recursiveUpload(s3Client, filePath, subBucket);
                }
                else
                {
                    var fileTransferUtility = new TransferUtility(s3Client);
                    await fileTransferUtility.UploadAsync(filePath, bucketName);
                }
            }
            catch (Exception)
            {
                Console.WriteLine("There was an exception uploading: " + filePath + " to the bucket");
                return;
            }
        }
    }
}