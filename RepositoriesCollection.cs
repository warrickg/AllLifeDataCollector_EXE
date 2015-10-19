using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AllLifeDataCollector_EXE
{
    class RepositoriesCollection : System.Collections.CollectionBase
    {
        public void Add(Repository aRepository)
        {
            List.Add(aRepository);
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

        public Repository Item(int Index)
        {
            return (Repository)List[Index];
        }

    }
}
