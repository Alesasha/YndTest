/*
Программа находит все простые числа в диапазоне от 1 до N. 
Ссылка на исполняемый модуль: https://yadi.sk/d/xo80iFLLUewz3g
Алгоритм работает следующим образом:
Последовательно рассматриваются все нечётные числа от 5 до N (текущее рассматриваемое значение хранится в curPN). 
Для каждого уже найденного простого числа, создаётся накопительная сумма, которая инкрементируется на величину 
этого простого числа, каждый раз, когда она оказывается меньше текущего рассматриваемого числа. 
Если все накопительные суммы оказываются больше текущего значения – то оно является простым и сохраняется в листе простых чисел.
Временную сложность в теории оценить не просто. Но в ходе тестов (в диапазоне от 1 000 000 до 1 024 000 000), 
удалось установить, что при увеличении N в два раза, время выполнения алгоритма возрастает не более чем в 2,4 раза 
(в среднем в 2,35). Примерно это можно выразить формулой: O(N*1.2621^ln(N)).
Вот результаты тестирования на моей машине:
6914400 мс (01:55:14.400)  N=1024000000 (52005086 prime numbers)
3026415 мс (00:50:26.415)  N=0512000000 (26953826 prime numbers)
1316675 мс (00:21:56.675)  N=0256000000 (13989538 prime numbers)
0543344 мс (00:09:03.344)  N=0128000000 (07271036 prime numbers)
0225570 мс (00:03:45.570)  N=0064000000 (03785087 prime numbers)
0094855 мс (00:01:34.855)  N=0032000000 (01973816 prime numbers)
0039958 мс (00:00:39.958)  N=0016000000 (01031131 prime numbers)
0016846 мс (00:00:16.846)  N=0008000000 (00539778 prime numbers)
0007116 мс (00:00:07.116)  N=0004000000 (00283147 prime numbers)
0003054 мс (00:00:03.054)  N=0002000000 (00148934 prime numbers)
0001345 мс (00:00:01.345)  N=0001000000 (00078499 prime numbers)
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace YndTest
{
    class Program
    {
        [DllImport("kernel32.dll")]
        static extern int GetCurrentProcessorNumber();

        [DllImport("kernel32.dll")]
        static extern UIntPtr SetThreadAffinityMask(IntPtr hThread, UIntPtr dwThreadAffinityMask);

        [DllImport("kernel32.dll")]
        static extern IntPtr GetCurrentThread();

        void Foo()
        {
            SetThreadAffinityMask(GetCurrentThread(), (UIntPtr)1);
            while (true) { }
        }



        public class Cval //: IComparable<PrimeNumber>
        {
            public ulong inc;
            public int i;

            public Cval(ulong pn, ushort[] ls) { inc = pn; i = 0; sta = ls; sLen = sta.Length; }
            public volatile ushort[] sta;
            public ulong res;
            public int sLen;

            public ulong Step()
            {
                var str = Volatile.Read(ref sta[i++]);
                //if (i >= sLen) i = 0;
                ulong r = str * inc;
                return r;
            }
        }

        public class CQElem
        {
            public int ind;
            public ulong mask;
            public CQElem(int i, ulong m) { this.ind = i; this.mask = m; }
        }
        public class PSieve
        {
            public ulong curPos;
            public ulong sum;
            //public volatile List<ushort> sieve;
            public volatile ushort[] sieve;
            public int icur;
            public int istep;
            public int ics;

            public PSieve(int s) { icur = istep = 0; sieve = new ushort[s]; ics = 0; }
            public void MoveNext(ushort sp)
            {
                curPos += sp;
                if (++icur >= sieve.Length) icur = 0;
            }
            public void Add(ushort cst)
            {
                sieve[ics++] = cst;
            }
        }
        public class CPG
        {
            public int curPG;
        }
        delegate void Scanner(object i);
        /*
                static bool CheckPN(ulong n, ulong[] pnl)
                {
                    foreach (var pn in pnl[2..])
                        if (n % pn == 0 && n != pn)
                            return false;
                    return true;
                }
        */

    unsafe static void Main(string[] args)
        {
            //var sta = new List<ulong>();

            var PrimeN = new List<ulong>();
            int topPNi;
            int spni;
            int iost;
            ulong prevPrime;
            ulong res;
            ulong sqres;

            var cores = Environment.ProcessorCount;


            var first20PNs = new ulong[] { 1, 2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101 };
            var first11Sis = new int[] { 0, 1, 2, 8, 48, 480, 5760, 92160, 1658880, 36495360, 1021870080};

            //PrimeNs = new SortedSet<ulong>();

            var PNDic = new Dictionary<ulong, Cval>();
            const ulong startSearch = 29; // Первое простое число, не включённое в стартовое сито 23

            for (topPNi = 0; first20PNs[topPNi] < startSearch && topPNi < first20PNs.Length; topPNi++) ;

            ulong topPN = startSearch;
            ulong topPN2 = topPN * topPN;

            var SPrev = new PSieve(2);
            SPrev.Add(2);
            SPrev.Add(4);
            SPrev.sum = 6;

            Stopwatch Timer = new Stopwatch();
            Timer.Start();
            for (var i = 3; i < topPNi; i++)
            {
                SPrev.curPos = first20PNs[i + 1];

                SPrev.istep = 0;
                SPrev.icur = 1;
                if (SPrev.icur >= SPrev.sieve.Length) SPrev.icur = 0;
                var SCur = new PSieve(first11Sis[i]);
                var curPN = first20PNs[i];
                var curPN2 = curPN * curPN;
                //var curPN2ini = curPN2;
                SCur.sum = 0;
                var lastPos = SPrev.curPos + SPrev.sum * curPN;
                ushort SPic;
                while (SPrev.curPos < lastPos)
                {
                    while (SPrev.curPos < curPN2)
                    {
                        SPic = SPrev.sieve[SPrev.icur];
                        SCur.sum += SPic;
                        SCur.Add(SPic);
                        SPrev.MoveNext(SPic);
                    }
                    curPN2 += SPrev.sieve[SPrev.istep++] * curPN;
                    if (curPN2 > lastPos) curPN2 = lastPos;
                    if (SPrev.istep >= SPrev.sieve.Length) SPrev.istep = 0;
                    SPic = SPrev.sieve[SPrev.icur];
                    SCur.sum += SPic;
                    SCur.sieve[SCur.ics - 1] += SPic;
                    SPrev.MoveNext(SPic);
                }
                //Console.WriteLine($"SPrev.curPos={SPrev.curPos- first20PNs[i + 1]};lastPos={lastPos};SCur.sieve.Length={SCur.sieve.Length}");
                SPrev = SCur;
            }

            Timer.Stop();
            Console.WriteLine($"Elapsed time: {Timer.ElapsedMilliseconds:### ### ##0}ms ({Timer.Elapsed:hh\\:mm\\:ss\\.ffff}), Max step={SPrev.sieve.Max()},Scount={SPrev.sieve.Length}");
            Timer.Reset();
            var primeSieve = SPrev.sieve.ToArray();
            Console.ReadLine();

            //var psM = primeSieve.Max();
            //var psLen = primeSieve.Length;
            //return;
            
            res = InputULong(" Please enter ulong N >");
            Console.WriteLine($"\n{res}");

            //ulong* bitArr = stackalloc ulong[(int)((res >> 7) + 1)];
            var bitArr = new ulong[(res >> 7) + 1];

            for (topPNi = 0; first20PNs[topPNi] < startSearch && topPNi < first20PNs.Length; topPNi++)
                if (res > first20PNs[topPNi] - 1) PrimeN.Add(first20PNs[topPNi]);
            //Console.WriteLine($"PrimeN.Count={PrimeN.Count}");
            iost = 0;
            prevPrime = startSearch;

            //cj = 0;
            //ckey = startSearch;
            //keyQ.Enqueue(startSearch);

            //Thread.CurrentThread
            //var Th1 = new Thread(AdjKey);
            //Th1.Start();
            //var Th2 = new Thread(AdjKey);
            //Th2.Start();


            //Console.WriteLine($"Total Cores={System.Environment.ProcessorCount}, CurrentCore={GetCurrentProcessorNumber()}");            
            //Console.ReadLine();

            //Stopwatch Timer = new Stopwatch();


            //Timer.Start();
            //Th1.Join();
            //------------------------------------------------Начало алгоритма

            sqres = (ulong)Math.Sqrt(res);

            int j = 0;
            for (ulong key = startSearch; key <= sqres; key += primeSieve[j++])
            {
                if (j >= primeSieve.Length) j = 0;
                if (PNDic.TryGetValue(key, out Cval val))
                {
                    PNDic.Remove(key);
                    ulong k = key + val.Step();
                    while (PNDic.ContainsKey(k)) k += val.Step();
                    PNDic.Add(k, val);
                    continue;
                }
                if (topPN2 != key)
                {
                    PrimeN.Add(key);
                    continue;
                }
                //Console.WriteLine($"PrimeN[{PrimeN.Count}]={key};topPN={topPN};topPN2={topPN2};iost={iost}");
                var nv = new Cval(topPN, primeSieve);
                while (prevPrime != topPN)
                {
                    prevPrime += primeSieve[iost++];
                    if (iost >= primeSieve.Length) iost = 0;
                }
                nv.i = iost;
                //Console.WriteLine($"iost={iost}");

                ulong ktmp;                                          //
                while (PNDic.ContainsKey((ktmp = key + nv.Step()))) ; // while (!PNDic.TryAdd(key + nv.Step(), nv)) ;
                PNDic.Add(ktmp, nv);                                 //

                topPN = PrimeN[++topPNi];
                topPN2 = topPN * topPN;
                //if(PNDic.Count>7)
                    //Console.WriteLine($"PNDic[957]={PNDic[957].inc}");
            }

            //goto M1;

            //int a = (int)(res >> 10);

            //var bitArr = new bool[(res >> 1) + 1];  //ulong[(res >> 7) + 1]
            //ulong* bitArr = stackalloc ulong[(int)((res >> 7) + 1)];
            //var bitArr = new ulong[(res >> 7) + 1];

            //for (var i = 0; i < bitArr.Length; i++) bitArr[i] = new bool[512];
            //int clusterSize = 1 << 20;

            //var bitArrUnion = new bool[(res>>21)+1][];
            //for (var i = 0; i < bitArrUnion.Length-1; i++) bitArrUnion[i] = new bool[clusterSize];
            //bitArrUnion[bitArrUnion.Length - 1] = new bool[((res>>1) % (ulong)clusterSize)+1];

            //IntPtr dptr = Marshal.AllocHGlobal((int)(res>>1));

            var iPNL = PrimeN.Count;

            ulong totPM;
            
            int si = 0;
            while (si < iPNL && PrimeN[si] != startSearch) si++;

            iost = 0;
            ulong cpn = PrimeN[si];

            for (var i = 0; i < primeSieve.Length; i++) primeSieve[i] >>= 1;


            var nva = new List<Cval>();
            for (var i1 = si; i1 < iPNL; i1++)
            {
                while (cpn < PrimeN[i1])
                { 
                    cpn += (ulong)(primeSieve[iost++] << 1);
                    if (iost >= primeSieve.Length) iost = 0;
                }
                 var nv = new Cval(PrimeN[i1], primeSieve);//.ToArray()
                nv.i = iost;
                nv.res = res;
                nva.Add(nv);
            }

            //Action<int,ulong> ApplyMask = (i,m) => { bitArr[i] |= m; };
            //Func<int, ulong, bool> MaskNotSet = (i,m) => (bitArr[i] & m) == 0;

            //var MaskToSet = new ConcurrentQueue<CQElem>();

            //var WriteRequest = new ConcurrentDictionary<int,int>();
            var options = new ParallelOptions() { MaxDegreeOfParallelism = 3 };


            var cpg = new CPG();
            cpg.curPG = iPNL - 1;

            //var adr = new List<int>[iPNL - si];
            //var mask = new List<ulong>[iPNL - si];

            //Parallel.For(0, 16, options, (jj) =>  //

            //Timer.Start();
            //delegate void Scanner(int i);
            Scanner thd = (jjo) =>
            {
                //var i = iPNL - 1 + si - jj;

                var jj = (int)jjo;
                int i;
                while (true)
                {
                    Cval nv;
                    //lock (cpg)
                    i = cpg.curPG--;

                    if (i >= si)
                        nv = nva[i - si];
                    else
                        return;

                    ulong key = nv.inc;
                    key *= key;
                    key >>= 1;
                    var r = nv.res >> 1;

                    //int curpr = GetCurrentProcessorNumber();
                    //Console.WriteLine($"{curpr}--{jj}");

                    while (key <= r) //+ nv.Step() ulong key = ii; key <= res; key += 0
                    {
                        bitArr[key >> 6] |= (ulong)1 << (int)((key) & 0b111111);
                        key += nv.Step();
                    }
                }
            };

            //int totA = 0;
            //int ka = 0;

            //Action mark = () => 
            //{
            //    int i;
            //    //int k=0;

            //    List<int> a=null;
            //    //List<ulong> m=null;

            //    while (true)
            //    {
            //        lock (adr)
            //        {
            //            for (i = 0; i < adr.Length; i++)
            //                if (adr[i] != null)
            //                {
            //                    a = adr[i];
            //                    //m = mask[i];
            //                    adr[i] = null;
            //                    //mask[i] = null;
            //                    break;
            //                }
            //        }
            //        if (a is null)
            //        {
            //            Thread.Sleep(1);
            //            continue;
            //        }

            //        totA += a.Count;
            //        for (i = 0; i < a.Count; i++)
            //            bitArr[a[i]] = true;
            //        ka++;
            //        if (ka >= iPNL - si)
            //            return;
            //        a = null;
            //        //m = null;
            //    }




            //};
            /*
            int Ths = 1;
            var ThArr = new Thread[Ths];
            for (var i = 0; i < Ths; i++)
            {
                ThArr[i] = new Thread(new ParameterizedThreadStart(thd));
                //ThArr[i].Priority = ThreadPriority.Highest;
                ThArr[i].Start(i);
            }
            */
            //Timer.Start();
            //var Tsm = new Thread(new ThreadStart(mark));
            //Tsm.Priority = ThreadPriority.Highest;
            //Tsm.Start();

            //for (var i = 0; i < Ths; i++) 
            //  ThArr[i].Join();

            Timer.Start();
            thd(0);
            //Timer.Start();

            //var Tsm = new Thread(new ThreadStart(mark));
            //Tsm.Start();

            //Tsm.Join();
            //mark();
            //Console.WriteLine($"{ka}-->{totA}");


            //MaskToSet.Enqueue(new CQElem(0, 0b11));
            var lastPN = PrimeN[iPNL - 1] >> 1;
            j = 0;
            totPM = (ulong)PrimeN.Count;

            totalPM = (ulong)PrimeN.Count;

            //goto M1;
            var rr = res >> 1;

            for (ulong key = startSearch >> 1; key <= rr; key += primeSieve[j++])
            {
                if (j >= primeSieve.Length) j = 0;
                if (key > lastPN)
                {
                    //if (basa != (int)(key >> 20))
                    //{
                    //    basa = (int)(key >> 20);
                    //    bitArr = bitArrUnion[basa];
                    //}
                    //if (!bitArr[(key & 0b_1111_1111_1111_1111_1111)]) //!bitArr[key >> 1] //(key >> 1) & 0b111111111]
                    if ((bitArr[key >> 6] & ((ulong)1 << (int)((key) & 0b111111))) == 0)
                    {
                        totPM++;
                        //PrimeN.Add(key);
                    }
                }
            }

        //------------------------------------------------Конец алгоритма
        M1:

            Timer.Stop();
            //totPM = (ulong)PrimeN.Count;

            /*

                                    for (var i = 0; i < 1000; i++) //PrimeN.Count
                                    {
                                        Console.Write(PrimeN[i]);
                                        if (i == PrimeN.Count - 1)
                                            break;
                                        Console.Write(", ");
                                    }
            */
            //totPM = (ulong)PrimeN.Count;

            Console.WriteLine($"\nTotal prime numbers: {totPM:### ### ###}"); //PrimeN.Count
            Console.WriteLine($"Elapsed time: {Timer.ElapsedMilliseconds:### ### ##0}ms ({Timer.Elapsed:hh\\:mm\\:ss\\.ffff})");

            //Console.WriteLine($"Elapsed time for adding new element into PNDic: {lt:### ### ##0}ms ");
            Console.ReadKey();

        }
        static ulong totalPM;
        static void GetPM(ulong startSearch, ulong res, List<ulong> sta, ulong lastPN, uint[] bitArr, List<ulong> PrimeN, int iost)
        {
            int j = iost;
            ulong tPM = 0; // (ulong)PrimeN.Count;
            for (ulong key = startSearch; key <= res; key += sta[j++])
            {
                if (j >= sta.Count) j = 0;
                if (key > lastPN)
                {
                    var hkey = key >> 1;
                    //Console.WriteLine(hkey >> 5);
                    if ((bitArr[hkey >> 5] & ((uint)(1 << (int)(hkey & 0b11111)))) == 0)
                    {
                        tPM++;
                        //PrimeN.Add(key);
                    }
                }
            }
            Console.WriteLine(tPM);
            totalPM += tPM;
            //return totPM;
        }

        static ulong HashF(ulong[] fpn, int spni, ulong CPN)
        {
            ulong host = 0;
            for (var i = 2; i < spni; i++)
            {
                host <<= 5;
                host |= CPN % fpn[i];
            }
            return host;
        }

        static ulong InputULong(string instr)
        {
            ulong res = 15485860;
            ConsoleKeyInfo key;
            string inpS = res.ToString();

            while (true)
            {
                Console.Write("_");
                Console.Clear();
                Console.Write($"{instr}{inpS}");
                if ((key = Console.ReadKey()).Key == ConsoleKey.Enter)
                    break;
                int keyInt = (char)key.Key;
                if (keyInt > 95) keyInt -= 48;
                if (keyInt >= (int)'0' && keyInt <= (int)'9')
                {
                    if (inpS.Length == 0 && keyInt == (int)'0')
                        continue;
                    inpS += (char)keyInt;
                    if (!UInt64.TryParse(inpS, out res))
                    {
                        inpS = inpS.Remove(inpS.Length - 1);
                        UInt64.TryParse(inpS, out res);
                    }
                    continue;
                }
                if (key.Key == ConsoleKey.Backspace)
                {
                    if (inpS.Length > 0)
                    {
                        inpS = inpS.Remove(inpS.Length - 1);
                        UInt64.TryParse(inpS, out res);
                        continue;
                    }
                }
            }
            return res;
        }

        /*

        static void GetPNarray(List<ulong> PrimeN, ulong N)
        {
            var PN = new List<PNumC>();

            if (N > 0) PrimeN.Add(1);
            if (N > 1) PrimeN.Add(2);
            if (N > 2) {PrimeN.Add(3); PN.Add(new PNumC(6)); PN[0].pnn = 9; }
            if (N > 4) { PrimeN.Add(5); PN.Add(new PNumC(10)); PN[1].pnn = 25; }
            if (N > 6) { PrimeN.Add(7); PN.Add(new PNumC(14)); PN[2].pnn = 7; }

            var sta = new int[] {4,2,4,2,4,6,2,6};
            int ind;
            int indMax = 2;
            ulong pnMax2 = 11;// PrimeN[indMax + 3];
            pnMax2 *= pnMax2;
            int i = 0;                        
            for (ulong key = 7; key <= N; key += (ulong)sta[i++])
            {
                if (i >= sta.Length) i = 0;
                ind = 2;
                
                while (true)
                {
                    var pn = PN[ind];

                    while (pn.pnn < key)
                        pn.pnn += pn.step;
                    if (pn.pnn == key) { pn.pnn += pn.step; break; }

                    if (++ind > indMax)
                    {
                        if (pnMax2 == key) //==
                        {
                            indMax++;
                            PNumC pncl;
                            PN.Add((pncl =(new PNumC((uint)PrimeN[indMax+2]))));
                            pncl.pnn = (uint)(pnMax2 + (pncl.step <<= 1));
                            pnMax2 = PrimeN[indMax + 3]; pnMax2 *= pnMax2;
                            break;
                        }
                        PrimeN.Add(key);
                        break;
                    }
                }
            }
        }
        */
    }
}
