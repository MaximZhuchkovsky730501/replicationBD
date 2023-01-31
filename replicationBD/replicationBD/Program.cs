using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Data.OracleClient;
using System.IO;

namespace replicationBD
{
    class Settings
    {
        private String login, password;
        private List<String> server_lines;

        public Settings()
        {
            try // конструкций для обработки исключений try - catch
            {
                String text = read_settings(); //чтение данных из файла
                int ind = text.IndexOf("begin") + 7;
                text = text.Substring(ind); //удаляем слово "begin", и  весь текст до него
                String[] lines = text.Split(new char[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries); // разбиваем текст по строкам
                login = lines[0].Split('/')[0]; // получаем логин
                password = lines[0].Split('/')[1]; // получаем пароль
                server_lines = new List<string>(); // создаём список с информацией о серверах
                foreach (String line in lines)
                    if (line != login + '/' + password)
                        server_lines.Add(line); //добовляем строки с информацией о серверах в список 
            }
            catch (Exception e)
            {
                Console.WriteLine("ошибка интерпритации данных из файла настроек, возможно данные записаны с ошибками или в другой последовательности\n" + e.ToString());
                Console.ReadKey(); // ожидание нажатия на любую клавишу, чтобы консоль не закрывалась автоматически
                System.Diagnostics.Process.GetCurrentProcess().Kill(); // завершение работы приложения
            }
        }

        public String get_login()
        {
            return login;
        }

        public String get_password()
        {
            return password;
        }

        public List<String> get_server_lines()
        {
            return server_lines;
        }

        static private String read_settings()
        {
            String file_text = null;
            try
            {
                FileStream file = File.OpenRead("settings.txt"); // открываем файл "settings.txt" в корневой папке приложения
                byte[] array = new byte[file.Length];
                int count = file.Read(array, 0, array.Length); // считываем байты из файла в массив "array", count - колличество успешно считанных байт
                file_text = System.Text.Encoding.UTF8.GetString(array); //декодируем байты в текст
                file.Close(); // закрываем файл
            }
            catch (DirectoryNotFoundException e)
            {
                Console.WriteLine("не удалось найти путь к файлу \"settings.txt\"\n" + e.ToString());
                Console.ReadKey();
                System.Diagnostics.Process.GetCurrentProcess().Kill();
            }
            catch (FileNotFoundException e)
            {
                Console.WriteLine("файл \"settings.txt\" не найден\n" + e.ToString());
                Console.ReadKey();
                System.Diagnostics.Process.GetCurrentProcess().Kill();
            }
            catch (FileLoadException e)
            {
                Console.WriteLine("не удалось загрузить файл \"settings.txt\"\n" + e.ToString());
                Console.ReadKey();
                System.Diagnostics.Process.GetCurrentProcess().Kill();
            }
            catch (Exception e)
            {
                Console.WriteLine("неизвестная ошибка при чтении файла \"settings.txt\"\n" + e.ToString());
                Console.ReadKey();
                System.Diagnostics.Process.GetCurrentProcess().Kill();
            }
            return file_text;
        }
    }

    class Server
    {
        public String name, ip, service_name;
        public Server(String n, String i, String s)
        {
            name = n; //имя подразделения
            ip = i; //ip-адрес для подключения
            service_name = s; //servise name базы
        }
    }

    class Program
    {
        struct Argument
        {
            public List<String> list;
            public Server svr;
            public String login, password;
        }

        struct Output
        {
            public String text;
            public int progress;
            public int count;
        }
        static Output output = new Output();

        static Object locker = new Object();

        static public void console_rewrite()
        {
            Console.Clear();
            Console.WriteLine(output.text);
            Console.Write('[');
            for (int i = 0; i < output.count; i++)
            {
                if (i < output.progress)
                    Console.Write('-');
                else
                    Console.Write('*');
            }
            Console.WriteLine("] " + ((double)output.progress / (double)output.count * 100).ToString("0.00") + "% выполнено");
        }

        static public List<String> init_script_update()
        {
            List<String> command = new List<String>(); //создаём список комманд
            command.Add("VERY BIG SQL SCRIPT");
            command.Add("COMMIT");
            return command;
        }

        static public List<Server> init_server_list(List<String> lines)
        {
            List<Server> server = new List<Server>(); //создаём список серверов
            foreach (String line in lines)
            {
                String[] words = line.Split('/'); //разделяем строку на подстроки по символу "/" (содержимое строки: "имя подразделения/ip-адрес для подключения/servise name базы")
                server.Add(new Server(words[0], words[1], words[2])); //создаём экземпляры класса Server и добавляем их в список
            }
            output.count = server.Count;
            return server; // возвращаем список серверов
        }

        static public void thread_body(Object obj)
        {
            Argument arg = (Argument)obj;
            Server serv = arg.svr;
            List<String> script_update = arg.list;
            String login = arg.login;
            String password = arg.password;
            try
            {
                script_update = init_script_update(); //получаем список команд
                OracleConnection con = new OracleConnection();  //текущее подключение                       \
                OracleCommand cmd = new OracleCommand();        //исполняемая команда                        > переменные необходимые для подключения к БД
                String connectionString;                        //стока для открытия БД (как в tnsnames)    /
                //Console.Write(serv.name);
                connectionString = "Data Source = (DESCRIPTION = " +
                                                   "(ADDRESS = (PROTOCOL = TCP)(HOST =  " + serv.ip + ")(PORT = 8888)) " +
                                                   "(CONNECT_DATA = " +
                                                     "(SERVICE_NAME =  " + serv.service_name + ") " +
                                                   ") " +
                                                   ");User Id = " + login + ";password=" + password;

                con.ConnectionString = connectionString;
                cmd.Connection = con;
                con.Open(); //подключется к БД с использованием ранее заданных свойств
                lock (locker)
                {
                    output.text = output.text + serv.name + " подключение установлено\n";
                    console_rewrite();
                }
                foreach (String str in script_update) //для каждой команды из списка
                {
                    try
                    {
                        cmd.CommandText = str; //определяем команду для выполнения на сервере 
                        OracleDataReader dr = cmd.ExecuteReader(0); //выполняем команду
                        dr.Read(); //получаем результат выполнения

                        cmd.CommandText = "commit"; //
                        dr = cmd.ExecuteReader(0);  //выполняем команду "commit"
                        dr.Read();                  //
                    }
                    catch (OracleException e)
                    {
                        lock (locker)
                        {
                            output.text = output.text + "\nПри выполнении " + cmd.CommandText + " на сервере " + serv.name + " возникла ошибка:\n" + e.ToString() + "\n";
                            console_rewrite();
                        }
                    }
                }
                con.Close(); //закрываем соединение
                lock (locker)
                {
                    output.text = output.text + serv.name + " репликация завершена\n";
                    output.progress++;
                    console_rewrite();
                }
            }
            catch (OracleException e)
            {
                lock (locker)
                {
                    output.text = output.text + "\nПри подключении к " + serv.name + " возникла ошибка. проверьте параметры подключения в файле настроек\n" + e.ToString() + "\n";
                    console_rewrite();
                }
            }
            catch (Exception e)
            {
                lock (locker)
                {
                    output.text = output.text + "\n" + e.ToString() + "\n";
                    console_rewrite();
                }
            }
        }

        static public void script_update_run(List<Server> server_list, String login, String password)
        {
            List<String> script_update = new List<String>();
            script_update = init_script_update(); //получаем список команд
            output.text = output.text + "Репликация БД, подождите...\n";
            console_rewrite();
            List<Thread> threads = new List<Thread>(); ;
            Argument arg = new Argument();
            foreach (Server serv in server_list) //для каждого сервера из списка
            {
                threads.Add(new Thread(new ParameterizedThreadStart(thread_body)));
                arg.list = script_update;
                arg.svr = serv;
                arg.login = login;
                arg.password = password;
                threads.Last().Start(arg);
            }
            foreach (Thread thread in threads)
            {
                thread.Join();
            }
        }

        static void Main(string[] args)
        {
            Settings settings = new Settings(); //получаем основные параметры
            List<Server> server_list = new List<Server>(); //создаём список серверов
            server_list = init_server_list(settings.get_server_lines()); //заполняем список серверов
            script_update_run(server_list, settings.get_login(), settings.get_password()); //выполняем скрипт на всех серверах
            Console.WriteLine("репликация успешно завершена на " + output.progress + " из " + output.count + " базах\nнажмите любую клавишу для выхода");
            Console.Read();
        }
    }
}
