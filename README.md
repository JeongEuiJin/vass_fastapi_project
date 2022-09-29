# VASS ML Run Server

## 도커로 이미지를 만들어 실행

```
fastapi==0.81.0
gunicorn==20.1.0
numpy==1.23.2
pandas==1.3.2
uvicorn==0.18.3
pyodbc==4.0.34
scikit-learn==1.0.2
```

.config_secret 폴더 생성
> .config_secret.
>   >settings_local.json (로컬 세팅)
<br>settings_proj.json (도커안 세팅)

<pre>
<code>
settings_local.json
{
  "fastapi": {
    "databases": {
        "USER": "",
        "PASSWORD": "",
        "HOST": "localhost",
        "PORT": "",
        "DRIVER": "ODBC Driver 17 for SQL Server"
    },
    "allowed_hosts" : [
      "*"
    ]
  }

}
</code>
</pre>