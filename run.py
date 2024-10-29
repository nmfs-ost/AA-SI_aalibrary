# Entry-point for running the app

from aalibrary import api_app, routes


if __name__ == '__main__':
    api_app.run(host='localhost', debug=True)
