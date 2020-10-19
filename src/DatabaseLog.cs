using Atomus.Control;
using Atomus.Database;
using Atomus.Service;
using System;
using System.Collections;
using System.Collections.Generic;

namespace Atomus.Log
{
    public class DatabaseLog : IAction
    {
        int Interval;
        System.Threading.Thread Thread;
        Queue Queue;
        bool isStop;
        
        public DatabaseLog()
        {
            try
            {
                this.Interval = this.GetAttributeInt("Interval");
            }
            catch (Exception)
            {
                this.Interval = 10000;
            }

            this.isStop = false;

            this.Queue = new Queue();

            this.Thread = new System.Threading.Thread(this.Run);
            this.Thread.IsBackground = true;

            this.Thread.Start();
        }

        event AtomusControlEventHandler IAction.BeforeActionEventHandler
        {
            add
            {
                throw new NotImplementedException();
            }

            remove
            {
                throw new NotImplementedException();
            }
        }
        event AtomusControlEventHandler IAction.AfterActionEventHandler
        {
            add
            {
                throw new NotImplementedException();
            }

            remove
            {
                throw new NotImplementedException();
            }
        }

        object IAction.ControlAction(ICore sender, AtomusControlArgs e)
        {
            Dictionary<string, object> pairs;
            QueueData queueDatas;

            try
            {
                if (e.Value is Dictionary<string, object>)
                {
                    pairs = e.Value as Dictionary<string, object>;

                    if (pairs.Count == 7)
                    {
                        queueDatas = new QueueData();
                        queueDatas.IP_ADDRESS = (string)pairs["IP_ADDRESS"];
                        queueDatas.USER_ID = (string)pairs["USER_ID"];
                        queueDatas.REQUESTER = (string)pairs["REQUESTER"];
                        queueDatas.REQUEST_BODY = (string)pairs["REQUEST_BODY"];
                        queueDatas.REQUEST_RESULT = (string)pairs["REQUEST_RESULT"];
                        queueDatas.START_DATETIME = (DateTime)pairs["START_DATETIME"];
                        queueDatas.END_DATETIME = (DateTime)pairs["END_DATETIME"];

                        this.Queue.Enqueue(queueDatas);
                    }
                }
                else
                {
                    if (e.Action == "Stop")
                        this.isStop = true;
                }

                return true;
            }
            catch (Exception ex)
            {
                return false;
            }
        }

        internal void Enqueue(QueueData queueData)
        {
            try
            {
                this.Queue.Enqueue(queueData);
            }
            catch (Exception)
            {
            }
        }

        private void Run()
        {
            try
            {
                while (!this.isStop)
                {
                    try
                    {
                        System.Threading.Thread.Sleep(this.Interval);

                        while (this.Queue.Count > 0)
                            this.Save((QueueData)this.Queue.Dequeue());
                    }
                    catch (Exception)
                    {
                    }
                }
            }
            catch (Exception)
            {
            }
        }


        internal IResponse Save(QueueData queueData)
        {
            IServiceDataSet serviceDataSet;

            if (queueData == null)
                return null;

            serviceDataSet = new ServiceDataSet
            {
                ServiceName = this.GetAttribute("ServiceName"),
                TransactionScope = false,
            };

            serviceDataSet["Log"].ConnectionName = this.GetAttribute("ConnectionName");
            serviceDataSet["Log"].CommandText = this.GetAttribute("ProcedureSave");

            serviceDataSet["Log"].AddParameter("@LOG_ID", DbType.Decimal, 18, "Log", "@LOG_ID");
            serviceDataSet["Log"].AddParameter("@IP_ADDRESS", DbType.NVarChar, 50);
            serviceDataSet["Log"].AddParameter("@USER_ID", DbType.NVarChar, 50);
            serviceDataSet["Log"].AddParameter("@REQUESTER", DbType.NVarChar, 50);
            serviceDataSet["Log"].AddParameter("@REQUEST_BODY", DbType.NText);
            serviceDataSet["Log"].AddParameter("@REQUEST_RESULT", DbType.NText);
            serviceDataSet["Log"].AddParameter("@DATETIME", DbType.DateTime);

            serviceDataSet["Log"].NewRow();
            serviceDataSet["Log"].SetValue("@LOG_ID", DBNull.Value);
            serviceDataSet["Log"].SetValue("@IP_ADDRESS", queueData.IP_ADDRESS);
            serviceDataSet["Log"].SetValue("@USER_ID", queueData.USER_ID);
            serviceDataSet["Log"].SetValue("@REQUESTER", queueData.REQUESTER);
            serviceDataSet["Log"].SetValue("@REQUEST_BODY", queueData.REQUEST_BODY);
            serviceDataSet["Log"].SetValue("@REQUEST_RESULT", queueData.REQUEST_RESULT);
            serviceDataSet["Log"].SetValue("@DATETIME", queueData.START_DATETIME);

            serviceDataSet["Log"].NewRow();
            serviceDataSet["Log"].SetValue("@LOG_ID", DBNull.Value);
            serviceDataSet["Log"].SetValue("@IP_ADDRESS", DBNull.Value);
            serviceDataSet["Log"].SetValue("@USER_ID", DBNull.Value);
            serviceDataSet["Log"].SetValue("@REQUESTER", DBNull.Value);
            serviceDataSet["Log"].SetValue("@REQUEST_BODY", DBNull.Value);
            serviceDataSet["Log"].SetValue("@REQUEST_RESULT", queueData.REQUEST_RESULT);
            serviceDataSet["Log"].SetValue("@DATETIME", queueData.END_DATETIME);

            return (this.CreateInstance("ServiceName") as IService).Request((ServiceDataSet)serviceDataSet);
        }

    }

    internal class QueueData
    {
        public string IP_ADDRESS = "";
        public string USER_ID = "";
        public string REQUESTER = "";
        public string REQUEST_BODY = "";
        public string REQUEST_RESULT = "";
        public DateTime START_DATETIME;
        public DateTime END_DATETIME;
    }
}
