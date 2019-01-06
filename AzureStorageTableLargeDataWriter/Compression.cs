namespace AzureStorageTableLargeDataWriter
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.IO.Compression;
    using Newtonsoft.Json;

    public class Compression
    {
        public static byte[] Zip(dynamic obj)
        {
            byte[] zippedBytes;
            using (MemoryStream msi = new MemoryStream())
            using (StreamWriter sw = new StreamWriter(msi))
            using (JsonWriter jw = new JsonTextWriter(sw))
            {
                var serializer = new JsonSerializer();
                serializer.Serialize(jw, obj);
                sw.Flush();
                msi.Position = 0;

                using (var mso = new MemoryStream())
                {
                    using (var gs = new GZipStream(mso, CompressionMode.Compress))
                    {
                        CopyTo(msi, gs);
                    }

                    zippedBytes = mso.ToArray();
                }
            }

            return zippedBytes;
        }

        public static T Unzip<T>(byte[] bytes)
        {
            T response = default(T);

            using (var mso = new MemoryStream())
            {
                using (var msi = new MemoryStream(bytes))
                using (var gs = new GZipStream(msi, CompressionMode.Decompress))
                {
                    CopyTo(gs, mso);
                }

                mso.Flush();
                mso.Position = 0;

                using (StreamReader sr = new StreamReader(mso))
                using (JsonReader jr = new JsonTextReader(sr))
                {
                    JsonSerializer serializer = new JsonSerializer();
                    response = serializer.Deserialize<T>(jr);
                }
            }

            return response;
        }

        private static void CopyTo(Stream src, Stream dest)
        {
            byte[] bytes = new byte[4096];
            int cnt;
            while ((cnt = src.Read(bytes, 0, bytes.Length)) != 0)
            {
                dest.Write(bytes, 0, cnt);
            }
        }
    }
}
