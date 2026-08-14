using System;
using System.Diagnostics;
using System.IO;

internal static class Program
{
    private static int Main()
    {
        var root = AppDomain.CurrentDomain.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        var script = Path.Combine(root, "scripts", "install_windows.ps1");
        if (!File.Exists(script))
        {
            Console.Error.WriteLine("Не найден scripts\\install_windows.ps1.");
            Console.Error.WriteLine("Запускайте Install.exe из папки проекта (git clone).");
            Pause();
            return 1;
        }

        Console.Title = "Контур Маркировка — установка";
        Console.WriteLine("Полная переустановка в папке:");
        Console.WriteLine(root);
        Console.WriteLine();

        var start = new ProcessStartInfo
        {
            FileName = "powershell.exe",
            Arguments = "-NoProfile -ExecutionPolicy Bypass -File \"" + script + "\" -Reinstall",
            WorkingDirectory = root,
            UseShellExecute = false,
        };

        using (var process = Process.Start(start))
        {
            if (process == null)
            {
                Console.Error.WriteLine("Не удалось запустить PowerShell.");
                Pause();
                return 1;
            }

            process.WaitForExit();
            if (process.ExitCode != 0)
            {
                Console.WriteLine();
                Console.WriteLine("Установка завершилась с ошибкой. Код: " + process.ExitCode);
                Pause();
            }
            else
            {
                Console.WriteLine();
                Console.WriteLine("Готово. Запускайте программу ярлыком на рабочем столе.");
                Pause();
            }

            return process.ExitCode;
        }
    }

    private static void Pause()
    {
        Console.WriteLine("Нажмите Enter...");
        try
        {
            Console.ReadLine();
        }
        catch
        {
        }
    }
}
