<?php
/**
 * @note
 * kafka类的autoload文件
 *
 * @author  yangmingchuan
 * @date    2015-07-03
 */

spl_autoload_register(function($className)
{
    $basePath = dirname(__FILE__) . '/';
    $classFile = $basePath . str_replace('\\', DIRECTORY_SEPARATOR, $className) . '.php';
    if (function_exists('stream_resolve_include_path')) {
        $file = stream_resolve_include_path($classFile);
    } else {
        foreach (explode(PATH_SEPARATOR, get_include_path()) as $path) {
            if (file_exists($path . '/' . $classFile)) {
                $file = $path . '/' . $classFile;
                break;
            }
        }
    }
    /* If file is found, store it into the cache, classname <-> file association */
    if (($file !== false) && ($file !== null)) {
        include $file;
        return;
    }
});
