"""
Example usage of felog.LogFeaturePipeline.
Run this script to see pipeline produce windowed features from sample lines.
"""
from felog.pipeline import LogFeaturePipeline

sample_logs = [
    "Dec 10 07:07:45 LabSZ sshd[24206]: Failed password for invalid user test9 from 52.80.34.196 port 36060 ssh2",
    "Dec 10 07:07:45 LabSZ sshd[24206]: Received disconnect from 52.80.34.196: 11: Bye Bye [preauth]",
    "Dec 10 07:08:28 LabSZ sshd[24208]: reverse mapping checking getaddrinfo for ns.marryaldkfaczcz.com [173.234.31.186] failed - POSSIBLE BREAK-IN ATTEMPT!",
    "Dec 10 07:08:28 LabSZ sshd[24208]: Invalid user webmaster from 173.234.31.186",
    "Dec 10 07:08:28 LabSZ sshd[24208]: input_userauth_request: invalid user webmaster [preauth]",
    "Dec 10 07:08:28 LabSZ sshd[24208]: pam_unix(sshd:auth): check pass; user unknown"
]

if __name__ == '__main__':
    p = LogFeaturePipeline(window_seconds=60, enable_logging=False)
    # Inject sample lines directly
    p.ingest_from_file(br"logs\Linux_2k.log".encode())
    df_parsed = p.parse()
    print('Parsed DataFrame:')
    print(df_parsed[['timestamp','host','process','message']].head())

    df_features = p.run()
    print('\nWindowed Features:')
    print(df_features)
