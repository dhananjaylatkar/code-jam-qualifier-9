import typing
from dataclasses import dataclass


@dataclass(frozen=True)
class Request:
    scope: typing.Mapping[str, typing.Any]

    receive: typing.Callable[[], typing.Awaitable[object]]
    send: typing.Callable[[object], typing.Awaitable[None]]


class RestaurantManager:
    def __init__(self):
        """Instantiate the restaurant manager.

        This is called at the start of each day before any staff get on
        duty or any orders come in. You should do any setup necessary
        to get the system working before the day starts here; we have
        already defined a staff dictionary.
        """
        self.staff = {}
        self.special = {}
        self.busy = set()

    async def __call__(self, request: Request):
        """Handle a request received.

        This is called for each request received by your application.
        In here is where most of the code for your system should go.

        :param request: request object
            Request object containing information about the sent
            request to your application.
        """
        _scope = request.scope
        match _scope["type"]:
            case "staff.onduty":
                self.staff[_scope["id"]] = request
                for spe in _scope["speciality"]:
                    _special = self.special.get(spe, set())
                    _special.add(_scope["id"])
                    self.special[spe] = _special
                    # print(spe, "$$$", self.special[spe])

            case "staff.offduty":
                for spe, sta in self.special.items():
                    if _scope["id"] in sta:
                        sta.remove(_scope["id"])
                self.staff.pop(_scope["id"])

            case "order":
                found = None
                if _scope["speciality"] in self.special:
                    for sta in self.special[_scope["speciality"]]:
                        # print("Found speciality")
                        if sta not in self.busy:
                            # print("Using speciality")
                            found = self.staff[sta]
                            break

                if not found:
                    for sta in self.staff.keys():
                        if sta not in self.busy:
                            found = self.staff[sta]
                            break

                self.busy.add(sta)
                full_order = await request.receive()
                await found.send(full_order)

                result = await found.receive()
                await request.send(result)

                self.busy.remove(sta)
