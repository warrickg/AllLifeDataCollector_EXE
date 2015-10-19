using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AllLifeDataCollector_EXE
{
    class JobCollection : System.Collections.CollectionBase
    {
        public void Add(Job aJob)
        {
            List.Add(aJob);
        }

        public void Remove(int index)
        {
            // Check to see if there is a widget at the supplied index.
            if (index > Count - 1 || index < 0)
            // If no widget exists, a messagebox is shown and the operation 
            // is cancelled.
            {
                //System.Windows.Forms.MessageBox.Show("Index not valid!");
            }
            else
            {
                List.RemoveAt(index);
            }
        }

        public Job Item(int Index)
        {
            return (Job)List[Index];
        }
    }
}
