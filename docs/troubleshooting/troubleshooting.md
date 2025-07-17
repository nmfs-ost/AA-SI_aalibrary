# Troubleshooting

## The `Google Cloud` Warning
>C:\Users\hannan.khan\AppData\Local\conda\conda\envs\aalibrary\Lib\site-packages\google\auth\_default.py:76: UserWarning: Your application has authenticated using end user credentials from Google Cloud SDK without a quota project. You might receive a "quota exceeded" or "API not enabled" error. See the following page for troubleshooting: https://cloud.google.com/docs/authentication/adc-troubleshooting/user-creds.
>
> warnings.warn(_CLOUD_SDK_CREDENTIALS_WARNING)

This is a common warning. It is possible to ignore it, or to actually [surpress it](https://github.com/googleapis/google-auth-library-python/issues/271#issuecomment-400186626) if you do not like it. Surpressing this warning is not reccommended since it will also lead to other, possibly helpful, warnings being surpressed as well.
