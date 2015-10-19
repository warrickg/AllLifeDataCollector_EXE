using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AllLifeDataCollector_EXE
{
    class Job
    {
        private Int32 jobID;
        private string jobName;
        private decimal jobInterval;
        private bool jobEnabled;

        public Job()
        {
        }

        public Job(Int32 _jobID, string _jobName, decimal _jobInterval, bool _jobEnabled)
        {
            this.jobID = _jobID;
            this.jobName = _jobName;
            this.jobInterval = _jobInterval;
            this.jobEnabled = _jobEnabled;
        }

        public Int32 JobID
        {
            get { return this.jobID; }
            set { this.jobID = value; }
        }

        public string JobName
        {
            get { return this.jobName; }
            set { this.jobName = value; }
        }

        public decimal JobInterval
        {
            get { return this.jobInterval; }
            set { this.jobInterval = value; }
        }

        public bool JobEnabled
        {
            get { return this.jobEnabled; }
            set { this.jobEnabled = value; }
        }
    }
}
